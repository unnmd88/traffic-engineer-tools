use core::error;

use crate::{
    Error,
    error::{PollError, PollErrorContext},
    polling::{AttemptConfig, Pollable, Response, poll},
    utils::get_elapsed_as_u64,
};
use chrono::{Local, Utc};
use tokio::time::{Duration, Instant, error::Elapsed, sleep, timeout};

pub struct Poller<A: Pollable> {
    adapter: A,
    config: AttemptConfig,
}

impl<A: Pollable> Poller<A> {
    pub fn new(adapter: A, config: AttemptConfig) -> Self {
        Self { adapter, config }
    }

    pub async fn poll(&self) -> Result<Response<A::Output>, PollError> {
        poll(&self.config, &self.adapter).await
    }
}
