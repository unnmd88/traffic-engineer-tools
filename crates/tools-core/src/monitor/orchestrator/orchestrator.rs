use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    time::{Duration, Instant},
};

use chrono::{DateTime, Local};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    monitor::{
        task::{
            MonitorSnapshot, SpecRevision, TaskEntity, TaskId, TaskSpecPayload, TaskStatus,
            TaskView,
        },
        usecase::{UseCase, UseCaseOutput},
    },
    polling::worker::{WorkerEvent, WorkerFinished},
};

use super::{error::OrchestratorError, supervisor::WorkersPool};

/// Всё, что может прийти в цикл.
enum Msg {
    Command(Command),
    Facts(WorkerEvent<UseCaseOutput>),
    Exited {
        task_id: TaskId,
        finished: WorkerFinished,
    },
    Built(BuildOutcome),
    Tick,
}

/// Команды извне (Application/API).
#[derive(Debug)]
enum Command {
    AddTask {
        spec: TaskSpecPayload,
        reply: oneshot::Sender<Result<TaskId, OrchestratorError>>,
    },
    RemoveTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    StartTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    StopTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    UpdateTask {
        task_id: TaskId,
        spec: TaskSpecPayload,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<MonitorSnapshot>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Причина изменения — для live/файла (сток фильтрует по ней).
#[derive(Clone, Debug)]
pub enum ChangeKind {
    Added,
    SpecUpdated,
    Starting,
    Started,
    Restarted,
    Stopped,
    Completed,
    Failed { reason: String },
    BuildFailed { reason: String },
    Polled,
}

/// Единое событие наружу (тот же тип для broadcast и mpsc).
#[derive(Clone, Debug)]
pub enum MonitorEvent {
    TaskChanged {
        task_id: TaskId,
        kind: ChangeKind,
        view: TaskView,
        at: DateTime<Local>,
    },
    TaskRemoved {
        task_id: TaskId,
        view: TaskView,
        at: DateTime<Local>,
    },
}

/// Исход сборки адаптера, приходящий из отдельной таски.
struct BuildOutcome {
    task_id: TaskId,
    revision: SpecRevision,
    result: Result<UseCase, OrchestratorError>,
}

pub struct Orchestrator {
    tasks: HashMap<TaskId, TaskEntity>,
    next_id: u64,
    workers: WorkersPool,

    cmd_rx: mpsc::Receiver<Command>,
    worker_event_rx: mpsc::Receiver<WorkerEvent<UseCaseOutput>>,
    worker_exit_rx: mpsc::Receiver<(TaskId, WorkerFinished)>,
    build_rx: mpsc::Receiver<BuildOutcome>,
    build_tx: mpsc::Sender<BuildOutcome>,
    tick: tokio::time::Interval,

    live_tx: broadcast::Sender<MonitorEvent>,
    hist_tx: Option<mpsc::Sender<MonitorEvent>>,

    pending_builds: HashSet<TaskId>,
}

impl Orchestrator {
    pub fn new(hist_tx: Option<mpsc::Sender<MonitorEvent>>) -> (Self, OrchestratorApi) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (worker_event_tx, worker_event_rx) = mpsc::channel(32);
        let (worker_exit_tx, worker_exit_rx) = mpsc::channel(32);
        let (build_tx, build_rx) = mpsc::channel::<BuildOutcome>(32);
        let (live_tx, _) = broadcast::channel(1024);

        let kernel = Self {
            tasks: HashMap::new(),
            next_id: 0,
            workers: WorkersPool::new(worker_event_tx, worker_exit_tx),
            cmd_rx,
            worker_event_rx,
            worker_exit_rx,
            build_tx,
            build_rx,
            tick: tokio::time::interval(Duration::from_secs(1)),
            live_tx: live_tx.clone(),
            hist_tx,
            pending_builds: HashSet::new(),
        };

        (kernel, OrchestratorApi { cmd_tx, live_tx })
    }

    #[tracing::instrument(name = "orchestrator", skip_all)]
    pub async fn run(mut self) {
        tracing::info!("orchestrator started");
        loop {
            let msg = tokio::select! {
                Some(c) = self.cmd_rx.recv()                 => Msg::Command(c),
                Some(f) = self.worker_event_rx.recv()        => Msg::Facts(f),
                Some((id, fin)) = self.worker_exit_rx.recv() => Msg::Exited { task_id: id, finished: fin },
                Some(b) = self.build_rx.recv()               => Msg::Built(b),
                _ = self.tick.tick()                         => Msg::Tick,
            };

            match self.apply(msg) {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(Some(ev)) => {
                    let _ = self.live_tx.send(ev.clone());
                    if let Some(tx) = &self.hist_tx {
                        let _ = tx.send(ev).await;
                    }
                }
                ControlFlow::Continue(None) => {}
            }
        }
        tracing::info!("orchestrator stopped");
    }

    /// Единственная точка, где меняется состояние.
    fn apply(&mut self, msg: Msg) -> ControlFlow<(), Option<MonitorEvent>> {
        match msg {
            Msg::Command(c) => self.on_command(c),
            Msg::Facts(f) => ControlFlow::Continue(self.on_facts(f)),
            Msg::Exited {
                task_id,
                finished,
            } => ControlFlow::Continue(self.on_exit(task_id, finished)),
            Msg::Built(b) => ControlFlow::Continue(self.on_built(b)),
            Msg::Tick => ControlFlow::Continue(self.on_tick()),
        }
    }

    fn on_command(&mut self, cmd: Command) -> ControlFlow<(), Option<MonitorEvent>> {
        match cmd {
            Command::AddTask { spec, reply } => {
                let id = TaskId(self.next_id);
                self.next_id += 1;
                self.tasks.insert(id, TaskEntity::new(id, spec));

                let view = TaskView::from(&self.tasks[&id]);
                let _ = reply.send(Ok(id));
                ControlFlow::Continue(Some(MonitorEvent::TaskChanged {
                    task_id: id,
                    kind: ChangeKind::Added,
                    view,
                    at: Local::now(),
                }))
            }
            Command::StartTask { task_id, reply } => {
                let res = self.start(&task_id);
                let _ = reply.send(res.as_ref().map(|_| ()).map_err(|e| e.clone()));
                ControlFlow::Continue(res.ok().flatten())
            }
            Command::StopTask { task_id, reply } => {
                let res = self.stop(&task_id);
                let _ = reply.send(res.as_ref().map(|_| ()).map_err(|e| e.clone()));
                ControlFlow::Continue(res.ok().flatten())
            }
            Command::UpdateTask {
                task_id,
                spec,
                reply,
            } => {
                let res = self.update(&task_id, spec);
                let _ = reply.send(res.as_ref().map(|_| ()).map_err(|e| e.clone()));
                ControlFlow::Continue(res.ok().flatten())
            }
            Command::RemoveTask { task_id, reply } => {
                let res = self.remove(&task_id);
                let _ = reply.send(res.as_ref().map(|_| ()).map_err(|e| e.clone()));
                ControlFlow::Continue(res.ok().flatten())
            }
            Command::GetSnapshot { reply } => {
                let _ = reply.send(self.snapshot());
                ControlFlow::Continue(None)
            }
            Command::Shutdown { reply } => {
                self.workers.stop_all();
                let _ = reply.send(());
                ControlFlow::Break(())
            }
        }
    }

    fn start(&mut self, id: &TaskId) -> Result<Option<MonitorEvent>, OrchestratorError> {
        // идемпотентность: уже работает или собирается → успех без события
        if self.workers.is_running(id) || self.pending_builds.contains(id) {
            return Ok(None);
        }
        let view = {
            let task = self
                .tasks
                .get_mut(id)
                .ok_or(OrchestratorError::TaskNotFound(*id))?;
            task.set_status(TaskStatus::Starting);
            task.reset_metrics();
            TaskView::from(&*task)
        };
        self.workers.reset_restart(id);
        self.schedule_build(*id);
        Ok(Some(MonitorEvent::TaskChanged {
            task_id: *id,
            kind: ChangeKind::Starting,
            view,
            at: Local::now(),
        }))
    }

    fn stop(&mut self, id: &TaskId) -> Result<Option<MonitorEvent>, OrchestratorError> {
        let view = {
            let task = self
                .tasks
                .get_mut(id)
                .ok_or(OrchestratorError::TaskNotFound(*id))?;
            task.set_status(TaskStatus::Stopped);
            TaskView::from(&*task)
        };
        self.workers.stop(id);
        Ok(Some(MonitorEvent::TaskChanged {
            task_id: *id,
            kind: ChangeKind::Stopped,
            view,
            at: Local::now(),
        }))
    }

    fn update(
        &mut self,
        id: &TaskId,
        spec: TaskSpecPayload,
    ) -> Result<Option<MonitorEvent>, OrchestratorError> {
        let was_running = self.workers.is_running(id);
        let view = {
            let task = self
                .tasks
                .get_mut(id)
                .ok_or(OrchestratorError::TaskNotFound(*id))?;
            task.update_spec(spec);
            if was_running {
                task.set_status(TaskStatus::Restarting);
            }
            TaskView::from(&*task)
        };
        if was_running {
            self.workers.stop(id);
            self.schedule_build(*id);
        }
        Ok(Some(MonitorEvent::TaskChanged {
            task_id: *id,
            kind: ChangeKind::SpecUpdated,
            view,
            at: Local::now(),
        }))
    }

    fn remove(&mut self, id: &TaskId) -> Result<Option<MonitorEvent>, OrchestratorError> {
        self.workers.stop(id);
        self.workers.remove(id);
        self.pending_builds.remove(id);
        let task = self
            .tasks
            .remove(id)
            .ok_or(OrchestratorError::TaskNotFound(*id))?;
        let view = TaskView::from(&task);
        Ok(Some(MonitorEvent::TaskRemoved {
            task_id: *id,
            view,
            at: Local::now(),
        }))
    }

    fn on_facts(&mut self, f: WorkerEvent<UseCaseOutput>) -> Option<MonitorEvent> {
        let id = TaskId(f.id.0);
        self.workers.reset_restart(&id);

        // Обычный опрос: задача жива. RatedLimit ставится в on_exit(Completed).
        let view = {
            let task = self.tasks.get_mut(&id)?;
            task.apply_poll(f.result, f.metrics, TaskStatus::Active);
            TaskView::from(&*task)
        };
        Some(MonitorEvent::TaskChanged {
            task_id: id,
            kind: ChangeKind::Polled,
            view,
            at: Local::now(),
        })
    }

    fn on_exit(&mut self, id: TaskId, finished: WorkerFinished) -> Option<MonitorEvent> {
        match finished {
            WorkerFinished::Completed => {
                self.workers.mark_stopped(&id);
                let view = {
                    let task = self.tasks.get_mut(&id)?;
                    task.set_status(TaskStatus::RatedLimit);
                    TaskView::from(&*task)
                };
                Some(MonitorEvent::TaskChanged {
                    task_id: id,
                    kind: ChangeKind::Completed,
                    view,
                    at: Local::now(),
                })
            }
            WorkerFinished::Failed(reason) => {
                self.workers.schedule_restart(&id)?;
                let view = {
                    let task = self.tasks.get_mut(&id)?;
                    task.set_status(TaskStatus::Restarting);
                    TaskView::from(&*task)
                };
                Some(MonitorEvent::TaskChanged {
                    task_id: id,
                    kind: ChangeKind::Failed { reason },
                    view,
                    at: Local::now(),
                })
            }
        }
    }

    fn on_built(&mut self, out: BuildOutcome) -> Option<MonitorEvent> {
        self.pending_builds.remove(&out.task_id);

        // задача удалена, пока шла сборка → результат в мусор
        let Some(task) = self.tasks.get(&out.task_id) else {
            return None;
        };
        // спека успела обновиться (даже N раз) → пересобрать один раз
        if out.revision != task.spec_revision() {
            self.schedule_build(out.task_id);
            return None;
        }

        match out.result {
            Ok(use_case) => {
                let (poll_config, metrics) = {
                    let task = self.tasks.get_mut(&out.task_id)?;
                    (*task.poll_config(), *task.metrics())
                };
                self.workers
                    .spawn(out.task_id, use_case, poll_config, metrics);
                let (kind, view) = {
                    let task = self.tasks.get_mut(&out.task_id)?;
                    // Восстановление после падения/update → Restarted, иначе первый запуск.
                    let kind = if *task.status() == TaskStatus::Restarting {
                        ChangeKind::Restarted
                    } else {
                        ChangeKind::Started
                    };
                    task.set_status(TaskStatus::Active);
                    (kind, TaskView::from(&*task))
                };
                Some(MonitorEvent::TaskChanged {
                    task_id: out.task_id,
                    kind,
                    view,
                    at: Local::now(),
                })
            }
            Err(e) => {
                let reason = e.to_string();
                let view = {
                    let task = self.tasks.get_mut(&out.task_id)?;
                    task.set_status(TaskStatus::Restarting);
                    TaskView::from(&*task)
                };
                self.workers.retry_later(&out.task_id);
                Some(MonitorEvent::TaskChanged {
                    task_id: out.task_id,
                    kind: ChangeKind::BuildFailed { reason },
                    view,
                    at: Local::now(),
                })
            }
        }
    }

    fn on_tick(&mut self) -> Option<MonitorEvent> {
        for id in self.workers.due_restarts(Instant::now()) {
            self.schedule_build(id);
        }
        None
    }

    /// Единственная точка запуска сборки адаптера (async, вне цикла).
    fn schedule_build(&mut self, task_id: TaskId) {
        if self.pending_builds.contains(&task_id) {
            return;
        }
        let Some(task) = self.tasks.get(&task_id) else {
            return;
        };
        let revision = task.spec_revision();
        let query = task.query().clone();
        let attempt = task.poll_config().attempt();

        self.pending_builds.insert(task_id);
        self.workers.mark_building(&task_id);

        let build_tx = self.build_tx.clone();
        tokio::spawn(async move {
            let result = UseCase::build(query, attempt).await.map_err(Into::into);
            let _ = build_tx
                .send(BuildOutcome {
                    task_id,
                    revision,
                    result,
                })
                .await;
        });
    }

    /// Проекция текущего состояния в read-model (чистое чтение).
    fn snapshot(&self) -> MonitorSnapshot {
        let mut tasks: Vec<TaskView> = self.tasks.values().map(TaskView::from).collect();
        tasks.sort_by_key(|v| v.id);
        MonitorSnapshot { tasks }
    }
}

#[derive(Clone)]
pub struct OrchestratorApi {
    cmd_tx: mpsc::Sender<Command>,
    live_tx: broadcast::Sender<MonitorEvent>,
}

impl OrchestratorApi {
    pub async fn add_task(&self, spec: TaskSpecPayload) -> Result<TaskId, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddTask { spec, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::RemoveTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StartTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StopTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskSpecPayload,
    ) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::UpdateTask {
                task_id,
                spec,
                reply: tx,
            })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::GetSnapshot { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.live_tx.subscribe()
    }

    /// Остановить оркестратор: гасит все воркеры и завершает цикл обработки.
    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Shutdown { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }
}
