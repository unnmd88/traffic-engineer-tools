use serde::Deserialize;
use tokio::time::Duration;

use tools_core::{
    error::Error,
    monitor::{
        application::Application,
        task::{QuerySnmpGet, RawSnmpOidItem, TaskSpec, UseCaseQuery},
    },
    polling::{AttemptConfig, PollConfig},
};

const DEFAULT_HISTORY_DEPTH: u8 = 3;

/// DTO конфига (serde на границе CLI; сырые строки).
#[derive(Debug, Deserialize)]
struct AppConfigDto {
    tasks: Vec<TaskConfigDto>,
}

#[derive(Debug, Deserialize)]
struct TaskConfigDto {
    name: String,
    interval_seconds: u64,
    limit: u64,
    #[serde(default)]
    deep_history: Option<u8>,
    attempt_config: AttemptPollTimingsDto,
    query: QueryDto,
}

#[derive(Debug, Deserialize)]
struct AttemptPollTimingsDto {
    timeout_ms: u64,
    retries: u8,
    retry_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
enum QueryDto {
    SnmpGet(SnmpGetQueryDto),
}

#[derive(Debug, Deserialize)]
struct SnmpGetQueryDto {
    #[serde(default)]
    profile: Option<String>,
    host: String,
    port: u16,
    community: String,
    oids: Vec<SnmpOidItemDto>,
}

#[derive(Debug, Deserialize)]
struct SnmpOidItemDto {
    #[serde(default)]
    name: Option<String>,
    oid: String,
}

pub struct AppBuilder;

impl AppBuilder {
    pub async fn from_yaml(content: &str) -> anyhow::Result<Application> {
        let dto: AppConfigDto = serde_yaml::from_str(content)?;
        let specs = dto
            .tasks
            .into_iter()
            .map(TaskSpec::try_from)
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Application::new(specs).await?)
    }
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

        let query = match dto.query {
            QueryDto::SnmpGet(q) => {
                let oids = q
                    .oids
                    .into_iter()
                    .map(|item| RawSnmpOidItem {
                        name: item.name,
                        oid: item.oid,
                    })
                    .collect();
                UseCaseQuery::SnmpGet(QuerySnmpGet::from_raw(
                    q.host,
                    q.port,
                    q.community,
                    q.profile,
                    oids,
                )?)
            }
        };

        Ok(TaskSpec {
            name: dto.name,
            query,
            poll_config,
            deep_history: dto.deep_history.unwrap_or(DEFAULT_HISTORY_DEPTH),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_into_validated_specs() {
        let yaml = r#"
tasks:
  - name: T-1
    interval_seconds: 5
    limit: 100
    attempt_config:
      timeout_ms: 1000
      retries: 2
      retry_delay_ms: 200
    query:
      query_type: snmpget
      host: 127.0.0.1
      port: 1161
      community: public
      oids:
        - oid: 1.3.6.1.4.1.1618.3.7.2.11.2
"#;

        let dto: AppConfigDto = serde_yaml::from_str(yaml).unwrap();
        let specs = dto
            .tasks
            .into_iter()
            .map(TaskSpec::try_from)
            .collect::<Result<Vec<_>, Error>>()
            .unwrap();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "T-1");
        assert_eq!(specs[0].poll_config.interval, Duration::from_secs(5));
    }
}
