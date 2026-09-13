use std::collections::HashMap;

use chrono::Local;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::monitor::{
    event::{ChangeKind, MonitorEvent, TaskFact},
    runtime::error::ProjectorError,
    task::{MonitorSnapshot, TaskId, TaskStatus, TaskView},
};

/// Команда к проектору (в отличие от `TaskFact` — не событие задачи).
enum ProjectorCmd {
    GetSnapshot {
        reply: oneshot::Sender<MonitorSnapshot>,
    },
}

/// Проекция: хранит последний снимок каждой задачи и раздаёт события наружу.
///
/// Единственный писатель в read-model. Не опрашивает и не считает статусы —
/// берёт готовый `TaskView` из факта актора.
pub struct Projector {
    views: HashMap<TaskId, TaskView>,
    fact_rx: mpsc::Receiver<TaskFact>,
    cmd_rx: mpsc::Receiver<ProjectorCmd>,
    live_tx: broadcast::Sender<MonitorEvent>,
    hist_tx: Option<mpsc::Sender<MonitorEvent>>,
}

impl Projector {
    pub fn new(hist_tx: Option<mpsc::Sender<MonitorEvent>>) -> (Self, ProjectorHandle) {
        let (fact_tx, fact_rx) = mpsc::channel(256);
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (live_tx, _) = broadcast::channel(1024);

        let this = Self {
            views: HashMap::new(),
            fact_rx,
            cmd_rx,
            live_tx: live_tx.clone(),
            hist_tx,
        };

        (
            this,
            ProjectorHandle {
                fact_tx,
                cmd_tx,
                live_tx,
            },
        )
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(fact) = self.fact_rx.recv() => self.apply(fact),
                Some(cmd) = self.cmd_rx.recv() => self.apply_cmd(cmd),
                else => break,
            }
        }
    }

    fn apply(&mut self, fact: TaskFact) {
        match fact {
            TaskFact::Changed {
                task_id,
                kind,
                view,
            } => {
                self.views.insert(task_id, view.clone());
                self.emit(MonitorEvent::TaskChanged {
                    task_id,
                    kind,
                    view,
                    at: Local::now(),
                });
            }
            TaskFact::Removed { task_id } => {
                if let Some(view) = self.views.remove(&task_id) {
                    self.emit(MonitorEvent::TaskRemoved {
                        task_id,
                        view,
                        at: Local::now(),
                    });
                }
            }
            TaskFact::Crashed { task_id, reason } => {
                if let Some(view) = self.views.get_mut(&task_id) {
                    view.status = TaskStatus::Failed;
                    let view = view.clone();
                    self.emit(MonitorEvent::TaskChanged {
                        task_id,
                        kind: ChangeKind::Failed { reason },
                        view,
                        at: Local::now(),
                    });
                }
            }
        }
    }

    fn apply_cmd(&mut self, cmd: ProjectorCmd) {
        match cmd {
            ProjectorCmd::GetSnapshot { reply } => {
                let _ = reply.send(self.snapshot());
            }
        }
    }

    fn emit(&self, ev: MonitorEvent) {
        if self.live_tx.receiver_count() > 0 {
            let _ = self.live_tx.send(ev.clone());
        }
        if let Some(tx) = &self.hist_tx {
            if tx.try_send(ev).is_err() {
                tracing::warn!("history channel full, event dropped");
            }
        }
    }

    fn snapshot(&self) -> MonitorSnapshot {
        let mut tasks: Vec<TaskView> = self.views.values().cloned().collect();
        tasks.sort_by_key(|v| v.id);
        MonitorSnapshot { tasks }
    }
}

/// Пульт проектора: куда акторы шлют факты, откуда берётся снапшот и подписка.
#[derive(Clone)]
pub struct ProjectorHandle {
    fact_tx: mpsc::Sender<TaskFact>,
    cmd_tx: mpsc::Sender<ProjectorCmd>,
    live_tx: broadcast::Sender<MonitorEvent>,
}

impl ProjectorHandle {
    /// Канал для фактов от акторов (и от супервизора — для Removed/Crashed).
    pub fn fact_sender(&self) -> mpsc::Sender<TaskFact> {
        self.fact_tx.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.live_tx.subscribe()
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, ProjectorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ProjectorCmd::GetSnapshot { reply: tx })
            .await
            .map_err(|_| ProjectorError::ChannelClosed)?;
        rx.await.map_err(|_| ProjectorError::ChannelClosed)
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use crate::{
        monitor::{
            event::{ChangeKind, TaskFact},
            runtime::Projector,
            task::{SpecRevision, TaskId, TaskStatus, TaskView},
        },
        polling::Metrics,
    };

    fn view(id: u64, status: TaskStatus) -> TaskView {
        TaskView {
            id: TaskId(id),
            name: format!("t-{id}"),
            revision: SpecRevision(1),
            target: String::new(),
            status,
            interval: Duration::from_secs(1),
            limit: 0,
            metrics: Metrics::default(),
            result: None,
            history: vec![],
        }
    }

    #[test]
    fn changed_orders_by_id() {
        let (mut p, _h) = Projector::new(None);
        p.apply(TaskFact::Changed {
            task_id: TaskId(2),
            kind: ChangeKind::Added,
            view: view(2, TaskStatus::Idle),
        });
        p.apply(TaskFact::Changed {
            task_id: TaskId(1),
            kind: ChangeKind::Added,
            view: view(1, TaskStatus::Idle),
        });
        let snap = p.snapshot();
        assert_eq!(snap.tasks.len(), 2);
        assert_eq!(snap.tasks[0].id, TaskId(1));
        assert_eq!(snap.tasks[1].id, TaskId(2));
    }

    #[test]
    fn removed_drops_view() {
        let (mut p, _h) = Projector::new(None);
        p.apply(TaskFact::Changed {
            task_id: TaskId(1),
            kind: ChangeKind::Added,
            view: view(1, TaskStatus::Idle),
        });
        p.apply(TaskFact::Removed { task_id: TaskId(1) });
        assert!(p.snapshot().tasks.is_empty());
    }

    #[test]
    fn crashed_marks_failed() {
        let (mut p, _h) = Projector::new(None);
        p.apply(TaskFact::Changed {
            task_id: TaskId(1),
            kind: ChangeKind::Started,
            view: view(1, TaskStatus::Active),
        });
        p.apply(TaskFact::Crashed {
            task_id: TaskId(1),
            reason: "boom".into(),
        });
        assert_eq!(p.snapshot().tasks[0].status, TaskStatus::Failed);
    }
}
