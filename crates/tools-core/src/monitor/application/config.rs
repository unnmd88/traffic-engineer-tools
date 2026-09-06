use serde::Deserialize;
use tokio::time::Duration;

use crate::{
    error::Error,
    monitor::task::{TaskSpec, UseCaseQuery},
    polling::{AttemptConfig, PollConfig},
};

const DEFAULT_HISTORY_DEPTH: u8 = 3;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub tasks: Vec<TaskConfigDto>,
}

#[derive(Debug, Deserialize)]
pub struct AttemptPollTimingsDto {
    pub timeout_ms: u64,
    pub retries: u8,
    pub retry_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct TaskConfigDto {
    pub name: String,
    pub interval_seconds: u64,
    pub limit: u64,
    #[serde(default)]
    pub deep_history: Option<u8>,
    pub attempt_config: AttemptPollTimingsDto,
    pub query: UseCaseQuery,
}

impl TryFrom<TaskConfigDto> for TaskSpec {
    type Error = Error;

    fn try_from(dto: TaskConfigDto) -> Result<Self, Self::Error> {
        let attempt = AttemptConfig {
            timeout: Duration::from_millis(dto.attempt_config.timeout_ms),
            retries: dto.attempt_config.retries,
            retry_delay: Duration::from_millis(dto.attempt_config.retry_delay_ms),
        };
        let poll_config = PollConfig {
            interval: Duration::from_secs(dto.interval_seconds),
            limit: dto.limit,
            attempt,
        };

        Ok(TaskSpec {
            name: dto.name,
            query: dto.query,
            poll_config,
            deep_history: dto.deep_history.unwrap_or(DEFAULT_HISTORY_DEPTH),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_config_dto_deserializes_and_converts_to_spec() {
        let json = r#"{
            "name": "T-1",
            "interval_seconds": 5,
            "limit": 100,
            "attempt_config": {
                "timeout_ms": 1000,
                "retries": 2,
                "retry_delay_ms": 200
            },
            "query": {
                "query_type": "snmpget",
                "host": "127.0.0.1",
                "port": 1161,
                "community": "public",
                "oids": [
                    {"oid": "1.3.6.1.4.1.1618.3.7.2.11.2"}
                ]
            }
        }"#;

        let dto: TaskConfigDto = serde_json::from_str(json).unwrap();
        let spec = TaskSpec::try_from(dto).unwrap();

        assert_eq!(spec.name, "T-1");
        assert_eq!(spec.poll_config.interval, Duration::from_secs(5));
        assert_eq!(spec.poll_config.limit, 100);
        assert_eq!(spec.poll_config.attempt.retries, 2);
        assert_eq!(spec.deep_history, DEFAULT_HISTORY_DEPTH);

        let UseCaseQuery::SnmpGet(q) = &spec.query;
        assert_eq!(q.host, "127.0.0.1");
        assert_eq!(q.port, 1161);
        assert!(q.profile.is_none());
        assert_eq!(q.oids.len(), 1);
        assert!(q.oids[0].name.is_none());
    }
}
