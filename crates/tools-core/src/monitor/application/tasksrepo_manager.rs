use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::error;

use crate::{
    Error,
    monitor::{
        TaskRepository, Uid,
        task::{TaskDataUpdateMessage, TaskId},
    },
    worker::{Metrics, TaskEvent, TaskResult},
};

#[derive(Debug, Clone)]
pub enum TasksRepoEvent {
    Update {
        snapshot: Arc<TaskRepository>,
        task_id: TaskId,
    },
}

#[derive(Debug)]
pub enum TasksRepoCommand {
    Update {
        task_id: TaskId,
        data: TaskDataUpdateMessage,
    },
    GetSnapShot {
        response: oneshot::Sender<TaskRepository>,
    },
}

pub struct TasksRepoManager {
    snapshot: TaskRepository,
    //group_mapping: HashMap<Uid, TaskGroupId>,
    //task_mapping: HashMap<Uid, TaskId>,
}

impl TasksRepoManager {
    pub fn new(
        snapshot: TaskRepository,
        //group_mapping: HashMap<Uid, TaskGroupId>,
        //task_mapping: HashMap<Uid, TaskId>,
    ) -> Self {
        Self {
            snapshot,
            //group_mapping,
            //task_mapping,
        }
    }

    pub async fn run(
        mut self,
        mut cmd_rx: mpsc::Receiver<TasksRepoCommand>,
        tx: broadcast::Sender<TasksRepoEvent>,
    ) {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                TasksRepoCommand::Update { task_id, data } => {
                    if let Err(e) = self.snapshot.update_taskstate(&task_id, data) {
                        error!(target: "TaskRepository Manager", task_id=?task_id, error = %e.to_string());
                    }

                    if tx.receiver_count() > 0 {
                        let event = TasksRepoEvent::Update {
                            snapshot: Arc::new(self.snapshot.clone()),
                            task_id: task_id.clone(),
                        };
                        if let Err(e) = tx.send(event) {
                            eprintln!("Failed to send event: {}", e);
                        }
                    }
                }
                TasksRepoCommand::GetSnapShot { response } => {
                    let _ = response.send(self.snapshot.clone());
                }
            }
        }
    }
}
