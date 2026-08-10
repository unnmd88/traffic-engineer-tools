use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::{
    monitor::{
        Snapshot, Uid,
        snapshot::UpdateTaskState,
        taskgroup::{TaskDataUpdateMessage, TaskGroupId, TaskPosition},
    },
    worker::{Metrics, TaskEvent, TaskResult},
};

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

    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<SnapshotCommand>) {
        while let Some(cmd) = cmd_rx.recv().await {
            println!("SnapshotManager: SnapshotCommand accepted");

            match cmd {
                SnapshotCommand::Update { uid, data } => {
                    let (group, task_position) = self.task_mapping.get(&uid).unwrap();
                    self.snapshot
                        .update_taskstate(group, task_position, data)
                        .unwrap();
                }
                SnapshotCommand::GetSnapShot { response } => {
                    let _ = response.send(self.snapshot.clone());
                }
            }
        }

        ()
    }
}
