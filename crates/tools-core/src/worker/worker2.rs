use tokio::time::Duration;
use tokio::{sync::mpsc, time::sleep};

use crate::error::PollError;
use crate::poll_response::Response;
use crate::polling::PollAdapter;
use crate::{
    Pollable, Updateble,
    worker::{Metrics, TaskEvent, TaskResult, WorkerCommand, WorkerId, WorkerState},
};

pub struct Worker2<A: Pollable + Updateble> {
    id: WorkerId,
    state: WorkerState,
    metrics: Metrics,
    tx: mpsc::Sender<TaskEvent>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
    poller: PollAdapter,
    adapter: A,
    interval: Duration,
}

impl<A: Pollable + Updateble<Instance = A>> Worker2<A>
where
    TaskResult: From<Response<A::Output>>,
{
    pub fn new(
        id: WorkerId,
        adapter: A,
        interval: Duration,
        poller: PollAdapter,
        tx: mpsc::Sender<TaskEvent>,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Self {
        Self {
            state: WorkerState::Idle,
            id,
            poller,
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

                        let raw_result = self.poller.run_poll(&self.adapter).await;

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
