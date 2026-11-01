use std::time::Duration;

use serde::Deserialize;

use tools_core::{
    error::Error,
    monitor::{
        probe::{ProbeSpec, RawSnmpOidItem, RawSnmpSetItem, SnmpGetSpec, SnmpSetSpec},
        task::TaskConfig,
    },
    polling::{AttemptConfig, PollConfig},
};

/// DTO конфига (serde на границе CLI; сырые строки).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfigDto {
    tasks: Vec<TaskConfigDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskConfigDto {
    name: String,
    #[serde(with = "humantime_serde")]
    interval: Duration,
    limit: u64,
    #[serde(default)]
    deep_history: Option<u8>,
    attempt_config: AttemptPollTimingsDto,
    query: QueryDto,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptPollTimingsDto {
    #[serde(with = "humantime_serde")]
    timeout: Duration,
    retries: u8,
    #[serde(with = "humantime_serde")]
    retry_delay: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
enum QueryDto {
    SnmpGet(SnmpGetQueryDto),
    SnmpSet(SnmpSetQueryDto),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnmpGetQueryDto {
    #[serde(default)]
    profile: Option<String>,
    host: String,
    port: u16,
    community: String,
    oids: Vec<RawSnmpOidItem>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnmpSetQueryDto {
    #[serde(default)]
    profile: Option<String>,
    host: String,
    port: u16,
    community: String,
    #[serde(default)]
    community_r: Option<String>,
    oids: Vec<RawSnmpSetItem>,
}

/// Загружает и валидирует задачи из YAML.
pub fn load_tasks_from_yaml(content: &str) -> anyhow::Result<Vec<TaskConfig>> {
    let de = serde_norway::Deserializer::from_str(content);
    let app_dto: AppConfigDto = serde_path_to_error::deserialize(de)?;
    let specs = app_dto
        .tasks
        .into_iter()
        .map(TaskConfig::try_from)
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(specs)
}

impl TryFrom<TaskConfigDto> for TaskConfig {
    type Error = Error;

    fn try_from(dto: TaskConfigDto) -> Result<Self, Self::Error> {
        let attempt = AttemptConfig::try_new(
            dto.attempt_config.timeout,
            dto.attempt_config.retries,
            dto.attempt_config.retry_delay,
        )?;
        let poll_config = PollConfig::try_new(dto.interval, dto.limit, attempt)?;

        let probe = match dto.query {
            QueryDto::SnmpGet(q) => {
                ProbeSpec::SnmpGet(SnmpGetSpec::from_raw(q.host, q.port, q.community, q.profile, q.oids)?)
            }
            QueryDto::SnmpSet(q) => ProbeSpec::SnmpSet(SnmpSetSpec::from_raw(
                q.host,
                q.port,
                q.community_r,
                q.community,
                q.profile,
                q.oids,
            )?),
        };

        Ok(TaskConfig::try_new(
            dto.name,
            probe,
            poll_config,
            dto.deep_history.unwrap_or_default(),
        )?)
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
    interval: 5s
    limit: 100
    attempt_config:
      timeout: 1s
      retries: 2
      retry_delay: 200ms
    query:
      query_type: snmpget
      host: 127.0.0.1
      port: 1161
      community: public
      oids:
        - oid: 1.3.6.1.4.1.1618.3.7.2.11.2
"#;

        let de = serde_norway::Deserializer::from_str(yaml);
        let dto: AppConfigDto = serde_path_to_error::deserialize(de).unwrap();
        let specs = dto
            .tasks
            .into_iter()
            .map(TaskConfig::try_from)
            .collect::<Result<Vec<_>, Error>>()
            .unwrap();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name(), "T-1");
        assert_eq!(specs[0].poll_config().interval(), Duration::from_secs(5));
    }
}
