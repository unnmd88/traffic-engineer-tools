use async_trait::async_trait;
use std::fmt::Debug;

use crate::error::UpdateError;

#[async_trait]
pub trait Updateble: Send + Sync {
    type Instance;

    async fn update(self) -> Result<Self::Instance, UpdateError>;
}
