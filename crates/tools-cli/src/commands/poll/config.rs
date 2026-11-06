use std::time::Duration;

use serde::Deserialize;

use tools_core::{
    error::Error,
    monitor::{
        probe::{RawSnmpOidItem, RawSnmpSetItem, SnmpGetSpec, SnmpSetSpec},
        scenario::{Scenario, SnmpGetScenarioSpec, SnmpSetScenarioSpec},
        task::TaskConfig,
    },
    polling::AttemptConfig,
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
        let interval = dto.interval;
        let limit = dto.limit;

        let scenario = match dto.query {
            QueryDto::SnmpGet(q) => {
                let request =
                    SnmpGetSpec::from_raw(q.host, q.port, q.community, q.profile, q.oids)?;
                Scenario::SnmpGet(SnmpGetScenarioSpec::try_new(request, interval, attempt)?)
            }
            QueryDto::SnmpSet(q) => {
                let request = SnmpSetSpec::from_raw(
                    q.host,
                    q.port,
                    q.community_r,
                    q.community,
                    q.profile,
                    q.oids,
                )?;
                Scenario::SnmpSet(SnmpSetScenarioSpec::try_new(request, interval, attempt)?)
            }
        };

        Ok(TaskConfig::try_new(
            dto.name,
            scenario,
            limit,
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
  - name: T-2
    interval: 5s
    limit: 0
    attempt_config:
      timeout: 1s
      retries: 0
      retry_delay: 0s
    query:
      query_type: snmpset
      profile: potok_ug405
      host: 127.0.0.1
      port: 1162
      community: private
      community_r: public
      oids:
        - oid: set_stage
          value: "3"
"#;

        let specs = load_tasks_from_yaml(yaml).unwrap();

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name(), "T-1");
        assert_eq!(specs[0].limit(), 100);
        let Scenario::SnmpGet(spec) = specs[0].scenario() else {
            panic!("expected SNMP GET scenario");
        };
        assert_eq!(spec.interval(), Duration::from_secs(5));
        assert_eq!(specs[1].name(), "T-2");
        assert_eq!(specs[1].limit(), 0);
        let Scenario::SnmpSet(spec) = specs[1].scenario() else {
            panic!("expected SNMP SET scenario");
        };
        let request = spec.request();
        assert_eq!(spec.interval(), Duration::from_secs(5));
        assert_eq!(request.port, 1162);
        assert_eq!(request.community.as_str(), "private");
        assert_eq!(request.community_r.as_ref().unwrap().as_str(), "public");
        assert!(matches!(
            &request.sets[0].value,
            tools_core::snmp::value::SnmpValue::OctetString(bytes) if bytes == &[4]
        ));
    }
}
