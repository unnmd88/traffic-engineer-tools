use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::error::PollError;
use crate::polling::config::PollConfig;
use crate::polling::worker::env::WorkerEvent;
use crate::polling::worker::{WorkerCommand, WorkerId, WorkerState};
use crate::polling::{Metrics, PollResult, Pollable, Response, Updateble, poll};

pub struct PollWorker<A: Pollable + Updateble> {
    id: WorkerId,
    state: WorkerState,
    metrics: Metrics,
    tx: mpsc::Sender<WorkerEvent>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
    poll_config: PollConfig,
    adapter: A,
    interval_tick: tokio::time::Interval,
}

impl<A: Pollable + Updateble<Instance = A>> PollWorker<A>
where
    PollResult: From<Response<A::Output>>,
{
    pub fn new(
        id: WorkerId,
        adapter: A,
        poll_config: PollConfig,
        tx: mpsc::Sender<WorkerEvent>,
        cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Self {
        Self {
            state: WorkerState::Idle,
            id,
            poll_config,
            metrics: Metrics::default(),
            adapter,
            interval_tick: tokio::time::interval(poll_config.interval),
            tx,
            cmd_rx,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }
               _ = self.interval_tick.tick() => {
                    self.handle_tick().await;
                }
            }
        }
    }

    async fn handle_tick(&mut self) {
        if !self.is_running() {
            return;
        }

        let poll_result = match poll(&self.poll_config.attempt, &self.adapter).await {
            Ok(response) => {
                self.metrics = self.metrics.with_success(response.elapsed);
                response.into()
            }
            Err(e) => {
                self.metrics = self.metrics.with_error();
                convert_error(e)
            }
        };

        if self.poll_config.limit > 0 && self.metrics.total_attempts >= self.poll_config.limit {
            self.state = WorkerState::RatedLimit;
        }

        let event = WorkerEvent {
            id: self.id,
            state: self.state,
            poll_config: self.poll_config.clone(),
            metrics: self.metrics.clone(),
            poll_result,
        };
        if self.tx.send(event).await.is_err() {
            tracing::warn!(target: "PollWorker", worker_id=?self.id, "Receiver dropped");
        }
    }

    fn transition_to_running(&mut self) {
        self.state = WorkerState::Running;
        self.metrics = Metrics::default();
        self.interval_tick = tokio::time::interval(self.poll_config.interval);
    }

    fn transition_to_resume(&mut self) {
        self.state = WorkerState::Running;
        self.interval_tick = tokio::time::interval(self.poll_config.interval);
    }

    fn transition_to_stop(&mut self) {
        self.state = WorkerState::Stopped;
    }

    fn set_limit(&mut self, limit: u64) {
        self.poll_config.limit = limit;
    }

    async fn handle_command(&mut self, cmd: Option<WorkerCommand>) {
        let Some(cmd) = cmd else {
            tracing::warn!(target: "worker", worker_id=?self.id, "Channel closed");
            return;
        };

        match self.state {
            WorkerState::Idle => self.handle_idle(cmd).await,
            WorkerState::Running => self.handle_running(cmd).await,
            WorkerState::Stopped => self.handle_stopped(cmd).await,
            WorkerState::RatedLimit => self.handle_rated_limit(cmd).await,
        }
    }

    async fn handle_idle(&mut self, cmd: WorkerCommand) {
        match cmd {
            WorkerCommand::Start => {
                self.transition_to_running();
                self.handle_tick().await;
            }
            _ => {
                tracing::warn!("Ignoring command {:?} in Idle", cmd);
            }
        }
    }

    async fn handle_running(&mut self, cmd: WorkerCommand) {
        match cmd {
            WorkerCommand::Stop => {
                self.transition_to_stop();
            }
            WorkerCommand::SetLimit(limit) => {
                self.set_limit(limit);
            }
            _ => {
                tracing::warn!("Ignoring command {:?} in Running", cmd);
            }
        }
    }

    async fn handle_stopped(&mut self, cmd: WorkerCommand) {
        match cmd {
            WorkerCommand::Start => {
                self.transition_to_running();
                self.handle_tick().await;
            }
            WorkerCommand::Resume => {
                self.transition_to_resume();
                //self.handle_tick().await;
            }
            _ => {
                tracing::warn!("Ignoring command {:?} in Stopped", cmd);
            }
        }
    }

    async fn handle_rated_limit(&mut self, cmd: WorkerCommand) {
        match cmd {
            WorkerCommand::Start => {
                self.transition_to_running();
                self.handle_tick().await;
            }
            WorkerCommand::SetLimit(limit) => {
                self.set_limit(limit);
            }
            _ => {
                tracing::warn!("Ignoring command {:?} in RatedLimit", cmd);
            }
        }
    }

    fn is_running(&self) -> bool {
        self.state == WorkerState::Running
    }
}

fn convert_error(e: PollError) -> PollResult {
    match e {
        PollError::NoResponse { errors } => PollResult::NoResponse(errors),
        PollError::Other { message } => PollResult::Fail { message },
    }
}
