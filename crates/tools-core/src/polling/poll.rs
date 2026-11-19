use chrono::{DateTime, Utc};
use tokio::time::{Instant, sleep, timeout};

use crate::polling::{
    AttemptConfig, AttemptError, AttemptOutcome, AttemptReport, PollOutcome, PollReport, Pollable,
};

/// Executes an action with up to `1 + retries` attempts and reports every attempt.
///
/// A non-retryable error stops the operation without discarding earlier attempts.
/// Durations and attempt offsets use a monotonic clock; UTC timestamps locate the
/// operation in external histories. Dropping this future or a panic does not return a report.
pub async fn poll<A: Pollable>(
    config: &AttemptConfig,
    adapter: &A,
) -> PollReport<A::Output, A::Error> {
    poll_observed(config, adapter, |_| {}).await
}

pub(crate) enum PollProgress<'a, E> {
    Started {
        at: DateTime<Utc>,
    },
    AttemptStarted {
        number: u16,
        started_after: tokio::time::Duration,
    },
    AttemptFinished {
        number: u16,
        report: &'a AttemptReport<E>,
    },
}

pub(crate) async fn poll_observed<A: Pollable, F>(
    config: &AttemptConfig,
    adapter: &A,
    mut observe: F,
) -> PollReport<A::Output, A::Error>
where
    F: FnMut(PollProgress<'_, A::Error>),
{
    let started_at = Utc::now();
    let started = Instant::now();
    observe(PollProgress::Started { at: started_at });
    let total_attempts = u16::from(config.retries()) + 1;
    let mut report = PollReport {
        started_at,
        finished_at: started_at,
        elapsed: tokio::time::Duration::ZERO,
        config: *config,
        attempts: Vec::with_capacity(usize::from(total_attempts)),
        outcome: PollOutcome::RetryLimitReached,
    };

    for attempt in 1..=total_attempts {
        let attempt_started = Instant::now();
        observe(PollProgress::AttemptStarted {
            number: attempt,
            started_after: attempt_started.duration_since(started),
        });
        let result = timeout(config.timeout(), adapter.poll()).await;
        let elapsed = attempt_started.elapsed();
        let (outcome, finished) = match result {
            Ok(Ok(payload)) => {
                report.outcome = PollOutcome::Succeeded(payload);
                (AttemptOutcome::Succeeded, true)
            }
            Ok(Err(AttemptError::NonRetryable(error))) => {
                report.outcome = PollOutcome::NonRetryableFailure;
                (
                    AttemptOutcome::Failed {
                        error,
                        retryable: false,
                    },
                    true,
                )
            }
            Ok(Err(AttemptError::Retryable(error))) => (
                AttemptOutcome::Failed {
                    error,
                    retryable: true,
                },
                false,
            ),
            Err(_) => (AttemptOutcome::TimedOut, false),
        };
        report.attempts.push(AttemptReport {
            started_after: attempt_started.duration_since(started),
            elapsed,
            outcome,
        });
        observe(PollProgress::AttemptFinished {
            number: attempt,
            report: &report.attempts[report.attempts.len() - 1],
        });
        if finished {
            break;
        }
        if attempt < total_attempts {
            sleep(config.retry_delay()).await;
        }
    }
    report.elapsed = started.elapsed();
    report.finished_at = Utc::now();
    report
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use tokio::time::Duration;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestError {
        Transport { code: u32 },
        Rejected { index: u32 },
    }

    struct Step {
        elapsed: Duration,
        result: Result<u32, AttemptError<TestError>>,
    }

    struct Scripted {
        steps: Mutex<VecDeque<Step>>,
    }

    impl Scripted {
        fn new(steps: Vec<Step>) -> Self {
            Self {
                steps: Mutex::new(steps.into()),
            }
        }
    }

    #[async_trait]
    impl Pollable for Scripted {
        type Output = u32;
        type Error = TestError;

        async fn poll(&self) -> Result<u32, AttemptError<TestError>> {
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra attempt");
            sleep(step.elapsed).await;
            step.result
        }
    }

    fn config(retries: u8) -> AttemptConfig {
        AttemptConfig::try_new(Duration::from_secs(1), retries, Duration::from_millis(100)).unwrap()
    }

    fn success(elapsed: Duration) -> Step {
        Step {
            elapsed,
            result: Ok(42),
        }
    }

    fn slow() -> Step {
        success(Duration::from_secs(10))
    }

    #[tokio::test(start_paused = true)]
    async fn first_success_reports_its_duration_and_does_not_retry() {
        let settings = config(3);
        let report =
            poll(&settings, &Scripted::new(vec![success(Duration::from_millis(40))])).await;
        assert_eq!(report.config, settings);
        assert_eq!(report.attempt_count(), 1);
        assert_eq!(report.retry_count(), 0);
        assert_eq!(report.outcome, PollOutcome::Succeeded(42));
        assert_eq!(report.attempts[0].outcome, AttemptOutcome::Succeeded);
        assert_eq!(report.attempts[0].started_after, Duration::ZERO);
        assert_eq!(report.attempts[0].elapsed, Duration::from_millis(40));
        assert_eq!(report.elapsed, Duration::from_millis(40));
        assert!(report.finished_at >= report.started_at);
    }

    #[tokio::test(start_paused = true)]
    async fn success_after_timeout_preserves_the_whole_timeline() {
        let report =
            poll(&config(2), &Scripted::new(vec![slow(), success(Duration::from_millis(40))]))
                .await;
        assert_eq!(report.outcome, PollOutcome::Succeeded(42));
        assert_eq!(report.attempt_count(), 2);
        assert_eq!(report.retry_count(), 1);
        assert_eq!(report.attempts[0].outcome, AttemptOutcome::TimedOut);
        assert_eq!(report.attempts[0].elapsed, Duration::from_secs(1));
        assert_eq!(report.attempts[1].started_after, Duration::from_millis(1100));
        assert_eq!(report.attempts[1].elapsed, Duration::from_millis(40));
        assert_eq!(report.elapsed, Duration::from_millis(1140));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_retries_still_reports_one_timed_out_attempt() {
        let report = poll(&config(0), &Scripted::new(vec![slow()])).await;
        assert_eq!(report.outcome, PollOutcome::RetryLimitReached);
        assert_eq!(report.attempt_count(), 1);
        assert_eq!(report.retry_count(), 0);
        assert_eq!(report.elapsed, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_timeouts_include_retry_delays_but_no_trailing_delay() {
        let settings = config(2);
        let report = poll(&settings, &Scripted::new(vec![slow(), slow(), slow()])).await;
        assert_eq!(report.outcome, PollOutcome::RetryLimitReached);
        assert_eq!(report.attempt_count(), 3);
        assert_eq!(report.elapsed, settings.budget());
        for (attempt, offset) in report.attempts.iter().zip([0, 1100, 2200]) {
            assert_eq!(attempt.started_after, Duration::from_millis(offset));
            assert_eq!(attempt.elapsed, settings.timeout());
            assert_eq!(attempt.outcome, AttemptOutcome::TimedOut);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_failure_keeps_timeouts_and_original_errors() {
        let adapter = Scripted::new(vec![
            slow(),
            Step {
                elapsed: Duration::from_millis(10),
                result: Err(AttemptError::Retryable(TestError::Transport { code: 7 })),
            },
            Step {
                elapsed: Duration::from_millis(20),
                result: Err(AttemptError::NonRetryable(TestError::Rejected { index: 3 })),
            },
        ]);
        let report = poll(&config(5), &adapter).await;
        assert_eq!(report.outcome, PollOutcome::NonRetryableFailure);
        assert_eq!(report.attempt_count(), 3);
        assert_eq!(report.attempts[0].outcome, AttemptOutcome::TimedOut);
        assert_eq!(
            report.attempts[1].outcome,
            AttemptOutcome::Failed {
                error: TestError::Transport { code: 7 },
                retryable: true
            }
        );
        assert_eq!(
            report.attempts[2].outcome,
            AttemptOutcome::Failed {
                error: TestError::Rejected { index: 3 },
                retryable: false
            }
        );
        assert_eq!(report.non_retryable_error(), Some(&TestError::Rejected { index: 3 }));
        assert_eq!(report.elapsed, Duration::from_millis(1230));
        let json = serde_json::to_value(&report).unwrap();
        let restored: PollReport<u32, TestError> = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), json);
    }

    #[tokio::test(start_paused = true)]
    async fn maximum_retries_allows_256_attempts() {
        let adapter = Scripted::new(
            (0..256)
                .map(|_| Step {
                    elapsed: Duration::ZERO,
                    result: Err(AttemptError::Retryable(TestError::Transport { code: 1 })),
                })
                .collect(),
        );
        let report = poll(&config(u8::MAX), &adapter).await;
        assert_eq!(report.outcome, PollOutcome::RetryLimitReached);
        assert_eq!(report.attempt_count(), 256);
        assert_eq!(report.retry_count(), 255);
    }
}
