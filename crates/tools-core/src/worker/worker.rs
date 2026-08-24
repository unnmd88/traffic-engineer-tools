use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, sleep};

use crate::error::PollError;
use crate::polling::{Poller, Response};
use crate::snmp::SnmpGetResponse;
use crate::utils::get_elapsed_as_u64;
use crate::worker::env::{TaskEvent, WorkerCommand, WorkerId, WorkerState};
use crate::worker::{Metrics, TaskResult};
use crate::{Error, PollErrorContext, Pollable};

pub struct Worker<P: Pollable> {
    id: WorkerId,
    poller: Poller<P>,
    interval: Duration,
}

impl<P: Pollable> Worker<P>
where
    TaskResult: From<Response<P::Output>>,
{
    pub fn new(id: WorkerId, poller: Poller<P>, interval: Duration) -> Self {
        Self {
            //state: WorkerState::Idle,
            id,
            poller,
            interval,
        }
    }

    pub async fn run(self, tx: mpsc::Sender<TaskEvent>, mut cmd_rx: mpsc::Receiver<WorkerCommand>) {
        let mut state = WorkerState::Idle;
        let mut metrics = Metrics::default();

        loop {
            //let start = Instant::now();
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    //println!("Worker {} accept command: {:?}", self.id, cmd);

                    match cmd {
                        Some(WorkerCommand::Stop) => {
                            state = WorkerState::Stopped;
                        }
                        Some(WorkerCommand::Start) => {
                            state = WorkerState::Running;
                        }
                        None => {
                            break; //TODO logging
                        }
                    }
                }
                _ = sleep(self.interval) => {
                    //println!("select! sleep. Worker {} state {state}", self.id);

                    if state == WorkerState::Running {

                        let raw_result = self.poller.poll().await;
                        let (updated_metrics,  res) = match raw_result {
                            Ok(response) => {
                                (metrics.with_success(response.elapsed), response.into())
                            }
                            Err(e) => {
                                (metrics.with_error(), convert_error(e))
                            }
                        };
                        metrics = updated_metrics;
                        //println!("Выполнена работа #{:#?}", &res);


                       if  tx.send(TaskEvent {
                              worker_id: self.id,
                              task_result: res,
                              metrics: metrics.clone(),
                              worker_state: state,
                            })
                            .await.is_err() {
                                eprintln!("Worker {}: receiver dropped", self.id);
                                return;
                        };
                    }

                }
            }
        }
    }
}

fn convert_error(e: PollError) -> TaskResult {
    match e {
        PollError::NoResponse { errors } => TaskResult::NoResponse(errors),
        PollError::Other { message } => TaskResult::Fail { message },
    }
}
