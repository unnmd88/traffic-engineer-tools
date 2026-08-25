use crate::{
    PollErrorContext, Pollable, error::PollError, poll_response::Response, polling::PollConfig,
};
use chrono::{Local, Utc};
use tokio::time::{Duration, Instant, error::Elapsed, sleep, timeout};

pub async fn poll<A: Pollable>(
    config: &PollConfig,
    adapter: &A,
) -> Result<Response<A::Output>, PollError> {
    let start = Instant::now();
    let mut errors = Vec::with_capacity(config.retries as usize);

    for attempt in 1..=config.retries {
        let attempt_result = timeout(config.timeout, adapter.poll()).await;
        let elapsed = start.elapsed();

        match attempt_result {
            Ok(Ok(payload)) => {
                return Ok(Response {
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

        if attempt < config.retries {
            sleep(config.retry_delay).await;
        }
    }
    Err(PollError::NoResponse { errors })
}
