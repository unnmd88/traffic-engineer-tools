use async_trait::async_trait;

use crate::error::PollError;

#[async_trait]
pub trait Pollable: Send + Sync {
    type Output: Send;

    async fn poll(&self) -> Result<Self::Output, PollError>;
}
