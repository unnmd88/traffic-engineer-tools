use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use super::AttemptConfig;

/// The complete report of one action, including all of its attempts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollReport<T, E> {
    /// Wall-clock time at which the operation started.
    pub started_at: DateTime<Utc>,
    /// Wall-clock time at which the operation finished.
    pub finished_at: DateTime<Utc>,
    /// Monotonic duration of the operation, including retry delays.
    pub elapsed: Duration,
    /// Attempt settings used by this operation.
    pub config: AttemptConfig,
    /// Every attempted execution, in chronological order.
    pub attempts: Vec<AttemptReport<E>>,
    /// The final result and the reason attempts stopped.
    pub outcome: PollOutcome<T>,
}

impl<T, E> PollReport<T, E> {
    /// Returns the number of attempted executions, including the first one.
    pub fn attempt_count(&self) -> usize {
        self.attempts.len()
    }

    /// Returns the number of executions after the first attempt.
    pub fn retry_count(&self) -> usize {
        self.attempt_count().saturating_sub(1)
    }

    /// Returns the error that stopped attempts without exhausting the retry limit.
    pub fn non_retryable_error(&self) -> Option<&E> {
        match (&self.outcome, &self.attempts.last()?.outcome) {
            (PollOutcome::NonRetryableFailure, AttemptOutcome::Failed { error, .. }) => Some(error),
            _ => None,
        }
    }
}

/// Timing and outcome of a single attempted execution of an action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptReport<E> {
    /// Monotonic offset from the start of the operation.
    pub started_after: Duration,
    /// Time spent executing the adapter, including response processing.
    pub elapsed: Duration,
    /// Success, timeout, or the adapter's typed error.
    pub outcome: AttemptOutcome<E>,
}

/// The outcome of one attempt; successful data is stored once in the operation outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptOutcome<E> {
    /// The action returned a result successfully.
    Succeeded,
    /// The attempt exceeded the timeout configured by the polling mechanism.
    TimedOut,
    /// The action returned an error.
    Failed {
        /// The original error returned by the adapter.
        error: E,
        /// Whether the adapter permits another attempt of this action.
        retryable: bool,
    },
}

/// The result of an operation and the reason its attempts stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PollOutcome<T> {
    /// An attempt succeeded; earlier attempts may have failed.
    Succeeded(T),
    /// All allowed attempts timed out or returned retryable errors.
    RetryLimitReached,
    /// An attempt returned an error that does not permit another attempt.
    NonRetryableFailure,
}
