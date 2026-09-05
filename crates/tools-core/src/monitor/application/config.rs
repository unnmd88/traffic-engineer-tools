use tokio::time::Duration;

use crate::{
    error::Error,
    monitor::task::{QuerySnmpGet, TaskSpec, UseCaseQuery},
    polling::{AttemptConfig, PollConfig},
};

const DEFAULT_HISTORY_DEPTH: u8 = 3;

#[derive(Debug)]
pub struct AppConfig {
    pub tasks: Vec<TaskConfigDto>,
}

#[derive(Debug)]
pub struct AttemptPollTimingsDto {
    pub timeout_ms: u64,
    pub retries: u8,
    pub retry_delay_ms: u64,
}

#[derive(Debug)]
pub struct TaskConfigDto {
    pub name: String,
    pub attempt_timings: AttemptPollTimingsDto,
    pub interval_ms: u64,
    pub limit: u64,
    pub deep_history: Option<u8>,
    pub query: Query,
}

#[derive(Debug)]
pub enum Query {
    SnmpGet(QuerySnmpGet),
}

impl TryFrom<TaskConfigDto> for TaskSpec {
    type Error = Error;

    fn try_from(dto: TaskConfigDto) -> Result<Self, Self::Error> {
        let attempt = AttemptConfig {
            timeout: Duration::from_millis(dto.attempt_timings.timeout_ms),
            retries: dto.attempt_timings.retries,
            retry_delay: Duration::from_millis(dto.attempt_timings.retry_delay_ms),
        };
        let poll_config = PollConfig {
            interval: Duration::from_millis(dto.interval_ms),
            limit: dto.limit,
            attempt,
        };

        let query = match dto.query {
            Query::SnmpGet(q) => UseCaseQuery::SnmpGet(q),
        };

        Ok(TaskSpec {
            name: dto.name,
            query,
            poll_config,
            deep_history: dto.deep_history.unwrap_or(DEFAULT_HISTORY_DEPTH),
        })
    }
}
