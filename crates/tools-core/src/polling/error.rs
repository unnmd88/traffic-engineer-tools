use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An adapter error together with its decision about retrying the action.
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum AttemptError<E> {
    /// Another attempt of the action is permitted.
    #[error("retryable error: {0}")]
    Retryable(E),
    /// Another attempt of the action is not permitted.
    #[error("non-retryable error: {0}")]
    NonRetryable(E),
}

/// Invalid attempt settings.
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigError {
    /// An attempt timeout must be positive.
    #[error("attempt timeout must be greater than zero")]
    TimeoutMustBePositive,
}
