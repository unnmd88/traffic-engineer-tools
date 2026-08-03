use async_trait::async_trait;
use std::fmt::Debug;

use crate::{Error, Payload};

#[async_trait]
pub trait Pollable: Send + Sync {
    type Output;

    async fn poll(&self) -> Result<Self::Output, Error>;
}
