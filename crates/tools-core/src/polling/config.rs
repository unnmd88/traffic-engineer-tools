use tokio::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct PollConfig {
    pub timeout: Duration,
    pub retries: u8,
    pub retry_delay: Duration,
}
