use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    monitor::{
        Snapshot, Uid,
        snapshot::UpdateTaskState,
        taskgroup::{TaskDataUpdateMessage, TaskGroupId, TaskPosition},
    },
    worker::{Metrics, TaskEvent, TaskResult},
};

#[derive(Debug, Clone)]
pub enum SnapshotEvent {
    Update {
        snapshot: Arc<Snapshot>,
        group_id: TaskGroupId,
        task_position: TaskPosition,
    },
}

#[derive(Debug)]
pub enum SnapshotCommand {
    Update {
        uid: Uid,
        data: TaskDataUpdateMessage,
    },
    GetSnapShot {
        response: oneshot::Sender<Snapshot>,
    },
}

pub struct SnapshotManager {
    snapshot: Snapshot,
    //group_mapping: HashMap<Uid, TaskGroupId>,
    task_mapping: HashMap<Uid, (TaskGroupId, TaskPosition)>,
}

impl SnapshotManager {
    pub fn new(
        snapshot: Snapshot,
        //group_mapping: HashMap<Uid, TaskGroupId>,
        task_mapping: HashMap<Uid, (TaskGroupId, TaskPosition)>,
    ) -> Self {
        Self {
            snapshot,
            //group_mapping,
            task_mapping,
        }
    }

    pub async fn run(
        mut self,
        mut cmd_rx: mpsc::Receiver<SnapshotCommand>,
        tx: broadcast::Sender<SnapshotEvent>,
    ) {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SnapshotCommand::Update { uid, data } => {
                    let (group_id, task_position) =
                        self.task_mapping.get(&uid).copied().expect("UID not found");

                    self.snapshot
                        .update_taskstate(&group_id, &task_position, data)
                        .unwrap();

                    if tx.receiver_count() > 0 {
                        let event = SnapshotEvent::Update {
                            snapshot: Arc::new(self.snapshot.clone()),
                            group_id,
                            task_position,
                        };
                        if let Err(e) = tx.send(event) {
                            eprintln!("Failed to send event: {}", e);
                        }
                    }
                }
                SnapshotCommand::GetSnapShot { response } => {
                    let _ = response.send(self.snapshot.clone());
                }
            }
        }
    }
}
