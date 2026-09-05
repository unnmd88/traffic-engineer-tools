use tokio::sync::mpsc;

use crate::error::PollError;
use crate::polling::config::PollConfig;
use crate::polling::worker::types::WorkerEvent;
use crate::polling::worker::WorkerId;
use crate::polling::{Metrics, PollResult, Pollable, Response, poll::poll};

pub struct PollWorker<A: Pollable> {
    id: WorkerId,
    poll_config: PollConfig,
    metrics: Metrics,
    adapter: A,
    event_tx: mpsc::Sender<WorkerEvent>,
    interval_tick: tokio::time::Interval,
}

impl<A: Pollable> PollWorker<A>
where
    PollResult: From<Response<A::Output>>,
{
    pub fn new(
        id: WorkerId,
        adapter: A,
        poll_config: PollConfig,
        event_tx: mpsc::Sender<WorkerEvent>,
        metrics: Metrics,
    ) -> Self {
        Self {
            id,
            poll_config,
            metrics,
            adapter,
            event_tx,
            interval_tick: tokio::time::interval(poll_config.interval),
        }
    }

    #[tracing::instrument(name = "poll_worker", skip_all, fields(worker_id = %self.id))]
    pub async fn run(mut self) {
        tracing::info!("worker started");

        loop {
            self.interval_tick.tick().await;

            // seed-метрики (после рестарта) могли уже исчерпать лимит
            if self.limit_reached() {
                break;
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

            let event = WorkerEvent {
                id: self.id,
                metrics: self.metrics,
                poll_result,
            };
            if self.event_tx.send(event).await.is_err() {
                tracing::warn!("receiver dropped");
                break;
            }

            if self.limit_reached() {
                tracing::info!(limit = self.poll_config.limit, "rate limit reached");
                break;
            }
        }
    }

    fn limit_reached(&self) -> bool {
        self.poll_config.limit > 0 && self.metrics.total_attempts >= self.poll_config.limit
    }
}

fn convert_error(e: PollError) -> PollResult {
    match e {
        PollError::NoResponse { errors } => PollResult::NoResponse(errors),
        PollError::Other { message } => PollResult::Fail { message },
    }
}
