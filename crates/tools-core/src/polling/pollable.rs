use async_trait::async_trait;

use crate::polling::error::AttemptError;

/// An action that the polling mechanism can execute, time out, and retry.
///
/// Dropping the returned future must allow a later attempt to use the adapter again.
#[async_trait]
pub trait Pollable: Send + Sync {
    /// The data produced by a successful execution of the action.
    type Output: Send;
    /// The adapter's original error type, retained in the operation report.
    type Error: Send;

    /// Executes the whole action once; the polling mechanism owns retries.
    async fn poll(&self) -> Result<Self::Output, AttemptError<Self::Error>>;
}
