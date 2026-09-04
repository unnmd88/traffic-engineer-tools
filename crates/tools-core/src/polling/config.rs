use tokio::time::Duration;

#[derive(Clone, Copy, Debug, Default)]
pub struct PollConfig {
    pub interval: Duration,
    pub limit: u64,
    pub attempt: AttemptConfig,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AttemptConfig {
    pub timeout: Duration,
    pub retries: u8,
    pub retry_delay: Duration,
}
