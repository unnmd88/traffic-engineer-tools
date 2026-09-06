use async_trait::async_trait;

use crate::polling::error::AttemptError;

#[async_trait]
pub trait Pollable: Send + Sync {
    type Output: Send;

    async fn poll(&self) -> Result<Self::Output, AttemptError>;
}
