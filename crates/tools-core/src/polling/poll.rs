use crate::polling::{
    AttemptConfig, AttemptError, FatalError, PollErrorContext, Pollable, Response,
};
use chrono::Local;
use tokio::time::{Instant, sleep, timeout};

/// Одна итерация опроса: `1 + retries` попыток. Ретраит только `Transient`,
/// `Fatal` сразу возвращает `Err`. `Success`/`NoResponse` — оба штатные исходы.
pub async fn poll<A: Pollable>(
    config: &AttemptConfig,
    adapter: &A,
) -> Result<Response<A::Output>, FatalError> {
    let started_at = Instant::now();
    // `retries` — число повторов ПОСЛЕ первой попытки.
    let total_attempts = config.retries().saturating_add(1);
    let mut errors = Vec::with_capacity(total_attempts as usize);

    for attempt in 1..=total_attempts {
        let attempt_started_at = Instant::now();
        let attempt_result = timeout(config.timeout(), adapter.poll()).await;

        match attempt_result {
            Ok(Ok(payload)) => {
                return Ok(Response::Success {
                    timestamp: Local::now(),
                    attempts: attempt,
                    errors,
                    elapsed: started_at.elapsed(),
                    payload,
                });
            }
            Ok(Err(AttemptError::Fatal(message))) => {
                return Err(FatalError { message });
            }
            Ok(Err(AttemptError::Transient(message))) => {
                errors.push(PollErrorContext {
                    attempt,
                    elapsed: attempt_started_at.elapsed(),
                    message,
                });
            }
            Err(_) => {
                errors.push(PollErrorContext {
                    attempt,
                    elapsed: attempt_started_at.elapsed(),
                    message: "Timeout".to_string(),
                });
            }
        }

        if attempt < total_attempts {
            sleep(config.retry_delay()).await;
        }
    }

    Ok(Response::NoResponse {
        timestamp: Local::now(),
        attempts: total_attempts,
        errors,
        elapsed: started_at.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Duration;

    struct AlwaysTransient {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Pollable for AlwaysTransient {
        type Output = ();
        async fn poll(&self) -> Result<(), AttemptError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(AttemptError::Transient("unreachable".to_string()))
        }
    }

    struct AlwaysFatal;

    #[async_trait]
    impl Pollable for AlwaysFatal {
        type Output = ();
        async fn poll(&self) -> Result<(), AttemptError> {
            Err(AttemptError::Fatal("fatal".to_string()))
        }
    }

    struct Succeeds;

    #[async_trait]
    impl Pollable for Succeeds {
        type Output = u32;
        async fn poll(&self) -> Result<u32, AttemptError> {
            Ok(42)
        }
    }

    fn cfg(retries: u8) -> AttemptConfig {
        AttemptConfig::try_new(
            Duration::from_millis(100),
            retries,
            Duration::from_millis(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn retries_zero_still_does_one_attempt() {
        let adapter = AlwaysTransient {
            calls: AtomicUsize::new(0),
        };
        let resp = poll(&cfg(0), &adapter).await.unwrap();
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(resp, Response::NoResponse { attempts: 1, .. }));
    }

    #[tokio::test]
    async fn retries_two_makes_three_attempts() {
        let adapter = AlwaysTransient {
            calls: AtomicUsize::new(0),
        };
        let resp = poll(&cfg(2), &adapter).await.unwrap();
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
        assert!(matches!(resp, Response::NoResponse { attempts: 3, .. }));
    }

    #[tokio::test]
    async fn fatal_returns_err_immediately() {
        let result = poll(&cfg(5), &AlwaysFatal).await;
        assert!(matches!(result, Err(FatalError { .. })));
    }

    #[tokio::test]
    async fn success_on_first_try() {
        let resp = poll(&cfg(3), &Succeeds).await.unwrap();
        assert!(matches!(
            resp,
            Response::Success {
                attempts: 1,
                payload: 42,
                ..
            }
        ));
    }
}
