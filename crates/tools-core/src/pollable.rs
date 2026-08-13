use async_trait::async_trait;
use std::fmt::Debug;

use crate::{Payload, error::PollError};

#[async_trait]
pub trait Pollable: Send + Sync {
    type Output;

    async fn poll(&self) -> Result<Self::Output, PollError>;
}
