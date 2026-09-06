use crate::polling::{
    AttemptConfig, AttemptError, FatalError, PollErrorContext, Pollable, Response,
};
use chrono::Local;
use tokio::time::{Instant, sleep, timeout};

pub async fn poll<A: Pollable>(
    config: &AttemptConfig,
    adapter: &A,
) -> Result<Response<A::Output>, FatalError> {
    let start = Instant::now();
    let mut errors = Vec::with_capacity(config.retries as usize);

    for attempt in 1..=config.retries {
        let attempt_result = timeout(config.timeout, adapter.poll()).await;
        let elapsed = start.elapsed();

        match attempt_result {
            Ok(Ok(payload)) => {
                return Ok(Response::Success {
                    timestamp: Local::now(),
                    attempts: attempt,
                    errors,
                    elapsed,
                    payload,
                });
            }
            Ok(Err(AttemptError::Fatal(message))) => {
                return Err(FatalError { message });
            }
            Ok(Err(AttemptError::Transient(message))) => {
                errors.push(PollErrorContext {
                    attempt,
                    elapsed,
                    message,
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

    Ok(Response::NoResponse {
        timestamp: Local::now(),
        attempts: config.retries,
        errors,
        elapsed: start.elapsed(),
    })
}
