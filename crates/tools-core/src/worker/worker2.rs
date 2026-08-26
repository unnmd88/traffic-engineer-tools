use tokio::time::Duration;
use tokio::{sync::mpsc, time::sleep};

use crate::error::PollError;
use crate::poll_response::Response;
use crate::polling::{PollConfig, poll};
use crate::worker::event::WorkerEvent;
use crate::{
    Pollable, Updateble,
    worker::{Metrics, TaskEvent, TaskResult, WorkerCommand, WorkerId, WorkerState},
};

pub struct PollWorker<A: Pollable + Updateble> {
    id: WorkerId,
    state: WorkerState,
    metrics: Metrics,
    tx: mpsc::Sender<WorkerEvent<A>>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
    poll_config: PollConfig,
    adapter: A,
    interval: Duration,
}

impl<A: Pollable + Updateble<Instance = A>> PollWorker<A>
where
    TaskResult: From<Response<A::Output>>,
{
    pub fn new(
        id: WorkerId,
        adapter: A,
        interval: Duration,
        poll_config: PollConfig,
        tx: mpsc::Sender<WorkerEvent<A>>,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Self {
        Self {
            state: WorkerState::Idle,
            id,
            poll_config,
            metrics: Metrics::default(),
            adapter,
            interval,
            tx,
            cmd_rx,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    //println!("Worker {} accept command: {:?}", self.id, cmd);

                    self.handle_command(cmd).await;
                }
                _ = sleep(self.interval) => {

                    if self.state == WorkerState::Running {


                        /*
                        let raw_result = poll(&self.poll_config, &self.adapter).await;

                        let (updated_metrics,  res) = match raw_result {
                            Ok(response) => {
                                (self.metrics.with_success(response.elapsed), response.into())
                            }
                            Err(e) => {
                                (self.metrics.with_error(), convert_error(e))
                            }
                        };
                        self.metrics = updated_metrics;
                        //println!("Выполнена работа #{:#?}", &res);
                        */

                       let event = match poll(&self.poll_config, &self.adapter).await {
                            Ok(response) => {
                                    self.metrics = self.metrics.with_success(response.elapsed);
                                    WorkerEvent::PollSuccess { worker_id: self.id, metrics: self.metrics, state: self.state, payload: response }

                                }
                            Err(error) => {
                                    self.metrics = self.metrics.with_error();
                                    WorkerEvent::PollError { worker_id: self.id, metrics: self.metrics, state: self.state, error: error }
                                }

                        };
                        if self.tx.send(event).await.is_err() {
                                tracing::warn!(target: "worker", worker_id=?self.id, "Receiver dropped");
                                return;

                            }

                        /*
                       if  self.tx.send(TaskEvent {
                              worker_id: self.id,
                              task_result: res,
                              metrics: self.metrics.clone(),
                              worker_state: self.state,
                            })
                            .await.is_err() {
                                tracing::warn!(target: "worker", worker_id=?self.id, "Receiver dropped");
                                return;
                        };
                        */
                    }

                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: Option<WorkerCommand>) {
        match cmd {
            Some(WorkerCommand::Stop) => {
                self.state = WorkerState::Stopped;
            }
            Some(WorkerCommand::Start) => {
                self.state = WorkerState::Running;
            }
            None => {
                tracing::warn!(target: "worker", worker_id=?self.id, "Channel closed");
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
