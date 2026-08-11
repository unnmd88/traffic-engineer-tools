use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::{
    monitor::{Uid, application::SnapshotCommand, taskgroup::TaskDataUpdateMessage},
    worker::{TaskEvent, WorkerId},
};

pub struct WorkerBridge {
    mapping: HashMap<WorkerId, Uid>,
}

impl WorkerBridge {
    pub fn new(mapping: HashMap<WorkerId, Uid>) -> Self {
        Self { mapping }
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<SnapshotCommand>,
        mut cmd_rx: mpsc::Receiver<TaskEvent>,
    ) {
        while let Some(result) = cmd_rx.recv().await {
            //println!("WorkerBridge: TaskEvent accepted");
            let uid = self.mapping.get(&result.worker_id).unwrap().clone();
            let update_msg = TaskDataUpdateMessage {
                metrics: result.metrics,
                task_result: result.task_result,
            };
            let cmd = SnapshotCommand::Update {
                uid,
                data: update_msg,
            };
            if let Err(_) = tx.send(cmd).await {
                eprintln!("Worker Brige receiver dropped");
                return;
            }
        }
    }
}
