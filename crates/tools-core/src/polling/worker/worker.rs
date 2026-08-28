use tokio::time::Duration;
use tokio::{sync::mpsc, time::sleep};

use crate::error::PollError;
use crate::polling::worker::env::WorkerEvent;
use crate::polling::worker::{WorkerCommand, WorkerId, WorkerState};
use crate::polling::{Metrics, PollConfig, PollResult, Pollable, Response, Updateble, poll};

pub struct PollWorker<A: Pollable + Updateble> {
    id: WorkerId,
    state: WorkerState,
    metrics: Metrics,
    tx: mpsc::Sender<WorkerEvent>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
    poll_config: PollConfig,
    adapter: A,
    interval: Duration,
}

impl<A: Pollable + Updateble<Instance = A>> PollWorker<A>
where
    PollResult: From<Response<A::Output>>,
{
    pub fn new(
        id: WorkerId,
        adapter: A,
        interval: Duration,
        poll_config: PollConfig,
        tx: mpsc::Sender<WorkerEvent>,
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
                       self.handle_command(cmd).await
                    }
                    _ = sleep(self.interval) => {
                        &self.handle_interval_tick().await;
                    }


            }
        }
    }

    async fn handle_interval_tick(&mut self) {
        if self.state != WorkerState::Running {
            return;
        }
        let poll_result = match poll(&self.poll_config, &self.adapter).await {
            Ok(response) => {
                self.metrics = self.metrics.with_success(response.elapsed);
                response.into()
            }
            Err(e) => {
                self.metrics = self.metrics.with_error();
                convert_error(e)
            }
        };

        let event = WorkerEvent {
            id: self.id,
            state: self.state,
            metrics: self.metrics.clone(),
            poll_result,
        };
        if self.tx.send(event).await.is_err() {
            tracing::warn!(target: "PollWorker", worker_id=?self.id, "Receiver dropped");
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

fn convert_error(e: PollError) -> PollResult {
    match e {
        PollError::NoResponse { errors } => PollResult::NoResponse(errors),
        PollError::Other { message } => PollResult::Fail { message },
    }
}
