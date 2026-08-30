use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, warn};

use crate::{
    Error,
    error::PollError,
    monitor::{
        TaskRepository,
        task::{PollStatus, TaskId},
        task_repository::TaskSnapshotUpdate,
    },
    polling::{
        PollResult, Pollable, Response,
        worker::{WorkerEvent, WorkerId, WorkerState},
    },
};

impl From<WorkerState> for PollStatus {
    fn from(state: WorkerState) -> Self {
        match state {
            WorkerState::Idle => Self::Idle,
            WorkerState::Running => Self::Active,
            WorkerState::Stopped => Self::Paused,
            WorkerState::Finished => Self::Finished,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TasksRepoResponse {
    Update {
        snapshot: Arc<TaskRepository>,
        task_id: TaskId,
    },
}

#[derive(Debug)]
pub enum TasksRepoCommand {
    GetSnapShot {
        response: oneshot::Sender<TaskRepository>,
    },
    SubscribeForUpdate {
        response: oneshot::Sender<broadcast::Receiver<TasksRepoResponse>>,
    },
}

pub struct TasksRepoManager {
    repository: TaskRepository,
    mapping: HashMap<WorkerId, TaskId>,
    tx: broadcast::Sender<TasksRepoResponse>,
    rx_cmd: mpsc::Receiver<TasksRepoCommand>,
    rx_worker: mpsc::Receiver<WorkerEvent>,
}

impl TasksRepoManager {
    pub fn new(
        repository: TaskRepository,
        mapping: HashMap<WorkerId, TaskId>,
        mut rx_cmd: mpsc::Receiver<TasksRepoCommand>,
        mut rx_worker: mpsc::Receiver<WorkerEvent>,
    ) -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            repository,
            tx,
            rx_cmd,
            rx_worker,
            mapping,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(worker_event) = self.rx_worker.recv() => {
                    self.handle_worker_event(worker_event).await;
                }
                Some(cmd) = self.rx_cmd.recv() => {
                    match cmd {
                        TasksRepoCommand::GetSnapShot { response } => {
                            let _ = response.send(self.repository.clone());
                        }
                        TasksRepoCommand::SubscribeForUpdate { response } => {
                             let rx = self.tx.subscribe();
                             let _ = response.send(rx);
                        }
                    }
                }
            }
        }
    }

    /*
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    TasksRepoCommand::Update { task_id, data } => {
                        if let Err(e) = self.repository.update_taskstate(&task_id, data) {
                            error!(target: "TaskRepository Manager", task_id=?task_id, error = %e.to_string());
                        }

                        if tx.receiver_count() > 0 {
                            let event = TasksRepoEvent::Update {
                                snapshot: Arc::new(self.repository.clone()),
                                task_id: task_id.clone(),
                            };
                            if let Err(e) = tx.send(event) {
                                eprintln!("Failed to send event: {}", e);
                            }
                        }
                    }
                    TasksRepoCommand::GetSnapShot { response } => {
                        let _ = response.send(self.repository.clone());
                    }
                }
            }
    */
    async fn handle_worker_event(&mut self, worker_event: WorkerEvent) {
        let task_id = match self.mapping.get(&worker_event.id) {
            Some(id) => id.clone(),
            None => {
                tracing::warn!(target: "handle_worker_event", worker_id=?worker_event.id, "Task for worker not found");
                return;
            }
        };

        match self.repository.update_task_snaphot(
            &task_id,
            TaskSnapshotUpdate {
                poll_status: worker_event.state.into(),
                metrics: worker_event.metrics,
                poll_result: worker_event.poll_result,
            },
        ) {
            Ok(_) => {
                if self.tx.receiver_count() > 0 {
                    let event = TasksRepoResponse::Update {
                        snapshot: Arc::new(self.repository.clone()),
                        task_id: task_id.clone(),
                    };
                    if let Err(e) = self.tx.send(event) {
                        error!(target: "send update event", "{}", e.to_string());
                    };
                }
            }
            Err(e) => {
                warn!(
                    target: "handle_worker_event",
                    worker_id=?worker_event.id,
                    task_id = ?task_id,
                    "{}", e.to_string());
            }
        }
    }
}
