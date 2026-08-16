use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::{
    monitor::{
        Uid,
        application::TasksRepoCommand,
        task::{TaskDataUpdateMessage, TaskId},
    },
    worker::{TaskEvent, WorkerId},
};

pub struct WorkerBridge {
    mapping: HashMap<WorkerId, TaskId>,
}

impl WorkerBridge {
    pub fn new(mapping: HashMap<WorkerId, TaskId>) -> Self {
        Self { mapping }
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<TasksRepoCommand>,
        mut cmd_rx: mpsc::Receiver<TaskEvent>,
    ) {
        while let Some(result) = cmd_rx.recv().await {
            //println!("WorkerBridge: TaskEvent accepted");
            let task_id = self.mapping.get(&result.worker_id).unwrap().clone(); //TODO Log
            let update_msg = TaskDataUpdateMessage {
                metrics: result.metrics,
                task_result: result.task_result,
            };
            let cmd = TasksRepoCommand::Update {
                task_id,
                data: update_msg,
            };
            if let Err(_) = tx.send(cmd).await {
                eprintln!("Worker Brige receiver dropped");
                return;
            }
        }
    }
}
