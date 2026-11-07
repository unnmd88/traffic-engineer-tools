use std::time::Duration;

use serde::Deserialize;

use tools_core::{
    error::Error,
    monitor::{
        probe::{RawSnmpOidItem, RawSnmpSetItem, SnmpGetSpec, SnmpSetSpec},
        scenario::{PollSchedule, ScenarioSpec, SnmpGetScenarioSpec, SnmpSetScenarioSpec},
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
    #[serde(default)]
    schedule: PollScheduleDto,
    limit: u64,
    #[serde(default)]
    deep_history: Option<u8>,
    attempt_config: AttemptPollTimingsDto,
    query: QueryDto,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PollScheduleDto {
    FixedRate,
    #[default]
    FixedDelay,
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
        let schedule = match dto.schedule {
            PollScheduleDto::FixedRate => PollSchedule::FixedRate {
                period: dto.interval,
            },
            PollScheduleDto::FixedDelay => PollSchedule::FixedDelay {
                delay: dto.interval,
            },
        };
        let limit = dto.limit;

        let scenario = match dto.query {
            QueryDto::SnmpGet(q) => {
                let request =
                    SnmpGetSpec::from_raw(q.host, q.port, q.community, q.profile, q.oids)?;
                ScenarioSpec::SnmpGet(SnmpGetScenarioSpec::try_new(request, schedule, attempt)?)
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
                ScenarioSpec::SnmpSet(SnmpSetScenarioSpec::try_new(request, schedule, attempt)?)
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
    schedule: fixed_rate
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
        let ScenarioSpec::SnmpGet(spec) = specs[0].scenario() else {
            panic!("expected SNMP GET scenario");
        };
        assert_eq!(
            spec.schedule(),
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(5)
            }
        );
        assert_eq!(specs[1].name(), "T-2");
        assert_eq!(specs[1].limit(), 0);
        let ScenarioSpec::SnmpSet(spec) = specs[1].scenario() else {
            panic!("expected SNMP SET scenario");
        };
        let request = spec.request();
        assert_eq!(
            spec.schedule(),
            PollSchedule::FixedRate {
                period: Duration::from_secs(5)
            }
        );
        assert_eq!(request.port, 1162);
        assert_eq!(request.community.as_str(), "private");
        assert_eq!(request.community_r.as_ref().unwrap().as_str(), "public");
        assert!(matches!(
            &request.sets[0].value,
            tools_core::snmp::value::SnmpValue::OctetString(bytes) if bytes == &[4]
        ));
    }

    fn task_yaml(schedule: &str, interval: &str) -> String {
        format!(
            "tasks:\n  - name: test\n    interval: {interval}\n    schedule: {schedule}\n    limit: 0\n    attempt_config:\n      timeout: 1s\n      retries: 2\n      retry_delay: 1s\n    query:\n      query_type: snmpget\n      host: 127.0.0.1\n      port: 161\n      community: public\n      oids: []\n"
        )
    }

    #[test]
    fn accepts_both_schedules_with_an_interval_shorter_than_the_request_budget() {
        for (mode, expected) in [
            (
                "fixed_rate",
                PollSchedule::FixedRate {
                    period: Duration::from_millis(100),
                },
            ),
            (
                "fixed_delay",
                PollSchedule::FixedDelay {
                    delay: Duration::from_millis(100),
                },
            ),
        ] {
            let tasks = load_tasks_from_yaml(&task_yaml(mode, "100ms")).unwrap();
            let ScenarioSpec::SnmpGet(spec) = tasks[0].scenario() else {
                panic!("expected SNMP GET scenario");
            };
            assert_eq!(spec.schedule(), expected);
        }
    }

    #[test]
    fn rejects_unknown_schedules() {
        assert!(load_tasks_from_yaml(&task_yaml("unknown", "1s")).is_err());
    }

    #[test]
    fn rejects_zero_duration_in_both_schedules() {
        for mode in ["fixed_rate", "fixed_delay"] {
            let error = load_tasks_from_yaml(&task_yaml(mode, "0s")).unwrap_err();
            assert!(matches!(
                error.downcast_ref::<Error>(),
                Some(Error::ScenarioSpec(
                    tools_core::monitor::scenario::ScenarioSpecError::ScheduleDurationMustBePositive
                ))
            ));
        }
    }
}
