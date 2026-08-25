use crate::{
    PollErrorContext, Pollable,
    error::PollError,
    poll_response::Response,
    polling::{PollConfig, poll},
};
use chrono::{Local, Utc};
use tokio::time::{Duration, Instant, error::Elapsed, sleep, timeout};

pub struct PollAdapter {
    config: PollConfig,
}

impl PollAdapter {
    pub fn new(config: PollConfig) -> Self {
        let capacity = config.retries as usize;
        Self { config }
    }

    pub fn config(&self) -> &PollConfig {
        &self.config
    }

    pub async fn run_poll<A: Pollable>(
        &self,
        adapter: &A,
    ) -> Result<Response<A::Output>, PollError> {
        poll(&self.config, adapter).await
    }
}
