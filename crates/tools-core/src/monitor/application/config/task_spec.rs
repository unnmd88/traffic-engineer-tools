use tokio::time::Duration;

use crate::{
    error::Error,
    monitor::application::config::{Query, TaskConfigDto, UseCaseQuery},
    polling::{AttemptConfig, PollConfig},
};

const DEFAULT_HISTORY_DEPTH: u8 = 3;

// Спека задачи — валидированное декларативное описание (Spec).
#[derive(Clone, Debug)]
pub struct TaskSpec {
    pub name: String,
    pub query: UseCaseQuery,
    pub poll_config: PollConfig,
    pub deep_history: u8,
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
