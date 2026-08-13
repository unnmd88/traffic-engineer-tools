use crate::{
    Error, Pollable,
    error::{PollError, PollErrorContext},
    polling::Response,
    utils::get_elapsed_as_u64,
};
use chrono::{Local, Utc};
use tokio::time::{Duration, Instant, error::Elapsed, sleep, timeout};

#[derive(Clone, Copy, Debug)]
pub struct PollConfig {
    pub timeout: Duration,
    pub retries: u8,
    pub retry_delay: Duration,
}

pub struct Poller<A: Pollable> {
    adapter: A,
    config: PollConfig,
}

impl<A: Pollable> Poller<A> {
    pub fn new(adapter: A, config: PollConfig) -> Self {
        Self { adapter, config }
    }

    pub async fn poll(&self) -> Result<Response<A::Output>, PollError> {
        let mut errors: Vec<PollErrorContext> = Vec::with_capacity(self.config.retries as usize);
        let start = Instant::now();

        let retries = self.config.retries;

        for attempt in 1..=self.config.retries {
            let attempt_result = timeout(self.config.timeout, self.adapter.poll()).await;
            let elapsed = start.elapsed();

            match attempt_result {
                Ok(Ok(payload)) => {
                    return Ok(Response {
                        //target,
                        //name,
                        elapsed,
                        timestamp: Local::now(),
                        errors,
                        attempts: attempt,
                        payload,
                    });
                }
                Ok(Err(e)) => {
                    errors.push(PollErrorContext {
                        attempt,
                        elapsed,
                        message: e.to_string(),
                    });
                }
                Err(_) => {
                    errors.push(PollErrorContext {
                        attempt,
                        elapsed,
                        message: "Timeout".to_string(),
                    });
                }
            }

            if attempt < self.config.retries {
                sleep(self.config.retry_delay).await;
            }
        }
        Err(PollError::NoResponse { errors })
    }
}
