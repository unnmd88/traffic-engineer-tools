use std::net::IpAddr;

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, sleep};

use crate::error::PollError;
use crate::polling::{Poller, Response};
use crate::snmp::SnmpGetResponse;
use crate::snmp::adapters::GenericCustomReader;
use crate::utils::get_elapsed_as_u64;
use crate::worker::Metrics;
use crate::{Error, PollErrorContext, Pollable};

use derive_more::{Display, Eq, Into};

#[derive(Display, Into, Clone, Copy, Hash, Eq, PartialEq)]
pub struct WorkerId(pub u64);

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    Running,
    Stopped,
    Finished,
}

#[derive(PartialEq, Eq)]
pub enum WorkerCommand {
    Start,
    Stop,
}

#[derive(PartialEq, Eq)]
pub enum WorkerResponse {
    CurrentState(WorkerState),
}

#[derive(Clone, Debug)]
pub enum TaskResult {
    Test(String),
    SnmpGet(Response<SnmpGetResponse>),
    NoResponseError(Vec<PollErrorContext>),
    OtherError { message: String },
}

impl From<Response<SnmpGetResponse>> for TaskResult {
    fn from(response: Response<SnmpGetResponse>) -> Self {
        TaskResult::SnmpGet(response)
    }
}

pub struct TaskEvent {
    pub worker_id: WorkerId,
    pub task_result: TaskResult,
    pub metrics: Metrics,
    pub worker_state: WorkerState,
}

pub struct Worker<P: Pollable> {
    id: WorkerId,
    state: WorkerState,
    poller: Poller<P>,
    interval: Duration,
}

impl<P: Pollable> Worker<P>
where
    TaskResult: From<Response<P::Output>>,
{
    pub fn new(
        id: WorkerId,
        poller: Poller<P>,
        interval: Duration,
        //repeat_config: WorkRimingConfig,
    ) -> Self {
        Self {
            state: WorkerState::Idle,
            id,
            poller,
            interval,
        }
    }

    pub async fn run(self, tx: mpsc::Sender<TaskEvent>, mut cmd_rx: mpsc::Receiver<WorkerCommand>) {
        let mut state = WorkerState::Running;
        //let mut attempts = 0u64;
        let mut metrics = Metrics::default();

        loop {
            //let start = Instant::now();
            tokio::select! {
                cmd = cmd_rx.recv() => {
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
                    if state == WorkerState::Running {

                    let raw_result = self.poller.poll().await;
                    let (updated_metrics,  res) = match raw_result {
                        Ok(response) => {
                            (metrics.with_success(response.elapsed), response.into())
                        }
                        Err(e) => {
                            (metrics.with_error(), task_result_from_error(e))
                        }
                    };
                    metrics = updated_metrics;

                       if  tx.send(TaskEvent {
                            worker_id: self.id,
                            task_result: res,
                            metrics: metrics.clone(),
                            worker_state: state,
                                })
                            .await.is_err() {
                                eprintln!("Worker {}: receiver dropped", self.id);
                                return;                            };
                    //println!("Выполнена работа #{}", attempts);
                    }

                }
            }
        }
    }
}

fn task_result_from_error(e: Error) -> TaskResult {
    match e {
        Error::Poll(PollError::NoResponse { errors }) => {
            println!("{:#?}", errors);
            TaskResult::NoResponseError(errors)
        }
        _ => TaskResult::OtherError {
            message: "OtherError".to_string(),
        },
    }
}
