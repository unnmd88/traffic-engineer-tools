use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::{
    Pollable,
    monitor::{
        application::TasksRepoCommand,
        task::{TaskDataUpdateMessage, TaskId},
    },
    worker::{TaskEvent, WorkerEvent, WorkerId},
};

pub struct WorkerBridge {
    mapping: HashMap<WorkerId, TaskId>,
}

impl WorkerBridge {
    pub fn new(mapping: HashMap<WorkerId, TaskId>) -> Self {
        Self { mapping }
    }

    pub async fn run<A: Pollable>(
        self,
        tx: mpsc::Sender<TasksRepoCommand>,
        mut cmd_rx: mpsc::Receiver<WorkerEvent<A>>,
    ) {
        while let Some(result) = cmd_rx.recv().await {
            //println!("WorkerBridge: TaskEvent accepted");
            continue;

            /*
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
            */
        }
    }
}
