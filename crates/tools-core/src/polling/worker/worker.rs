use tokio::sync::mpsc;

use crate::polling::config::PollConfig;
use crate::polling::worker::types::{WorkerEvent, WorkerFinished};
use crate::polling::worker::WorkerId;
use crate::polling::{Metrics, Pollable, Response, poll::poll};

pub struct PollWorker<A: Pollable> {
    id: WorkerId,
    poll_config: PollConfig,
    metrics: Metrics,
    adapter: A,
    event_tx: mpsc::Sender<WorkerEvent<A::Output>>,
    interval_tick: tokio::time::Interval,
}

impl<A: Pollable> PollWorker<A> {
    pub fn new(
        id: WorkerId,
        adapter: A,
        poll_config: PollConfig,
        event_tx: mpsc::Sender<WorkerEvent<A::Output>>,
        metrics: Metrics,
    ) -> Self {
        Self {
            id,
            poll_config,
            metrics,
            adapter,
            event_tx,
            interval_tick: tokio::time::interval(poll_config.interval()),
        }
    }

    #[tracing::instrument(name = "worker", skip_all, fields(worker_id = %self.id))]
    pub async fn run(mut self) -> WorkerFinished {
        tracing::info!("worker started");

        loop {
            self.interval_tick.tick().await;

            // seed-метрики (после рестарта) могли уже исчерпать лимит
            if self.limit_reached() {
                return WorkerFinished::Completed;
            }

            match poll(&self.poll_config.attempt(), &self.adapter).await {
                Ok(response) => {
                    self.metrics = update_metrics(self.metrics, &response);
                    let event = WorkerEvent {
                        id: self.id,
                        metrics: self.metrics,
                        result: response,
                    };
                    if self.event_tx.send(event).await.is_err() {
                        tracing::warn!("receiver dropped");
                        return WorkerFinished::Completed;
                    }
                }
                Err(fatal) => {
                    return WorkerFinished::Failed(fatal.message);
                }
            }

            if self.limit_reached() {
                tracing::info!(limit = self.poll_config.limit(), "rate limit reached");
                return WorkerFinished::Completed;
            }
        }
    }

    fn limit_reached(&self) -> bool {
        self.poll_config.limit() > 0 && self.metrics.total_attempts >= self.poll_config.limit()
    }
}

fn update_metrics<T>(metrics: Metrics, response: &Response<T>) -> Metrics {
    match response {
        Response::Success { elapsed, .. } => metrics.with_success(*elapsed),
        Response::NoResponse { .. } => metrics.with_error(),
    }
}
