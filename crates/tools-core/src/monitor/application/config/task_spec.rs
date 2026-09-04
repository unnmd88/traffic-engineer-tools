use tokio::time::Duration;

use crate::{
    error::Error,
    monitor::{
        application::config::{Query, TaskConfigDto, UseCaseQuery},
        task::{Protocol, TaskAttemptPollConfig, TaskMeta, TaskPollConfig, TypeQuery},
    },
};

// Спека задачи (готовая к сборке)
#[derive(Clone, Debug)]
pub struct TaskSpec {
    pub meta: TaskMeta,
    pub poll_config: TaskPollConfig,
    pub query: UseCaseQuery,
}

impl TryFrom<TaskConfigDto> for TaskSpec {
    type Error = Error;

    fn try_from(dto: TaskConfigDto) -> Result<Self, Self::Error> {
        let attempt = TaskAttemptPollConfig {
            timeout: Duration::from_millis(dto.attempt_timings.timeout_ms),
            retries: dto.attempt_timings.retries,
            retry_delay: Duration::from_millis(dto.attempt_timings.retry_delay_ms),
        };
        let poll_config = TaskPollConfig {
            interval: Duration::from_millis(dto.interval_ms),
            limit: dto.limit,
            attempt,
        };

        let (query, protocol, type_query, target, subject) = match dto.query {
            Query::SnmpGet(q) => {
                let target = format!("{}:{}", q.host, q.port);
                let subject = format!(
                    "Snmp-get request. Oids to request({}):\n{}",
                    q.oids.len(),
                    q.oids
                        .iter()
                        .map(|o| {
                            format!(
                                " - {}{}",
                                o.oid,
                                o.name
                                    .clone()
                                    .map_or_else(String::new, |n| format!(" [{n}]"))
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                (
                    UseCaseQuery::SnmpGet(q),
                    Protocol::Snmp,
                    TypeQuery::SnmpGet,
                    target,
                    subject,
                )
            }
        };

        let meta = TaskMeta {
            protocol,
            type_query,
            name: dto.name,
            target,
            subject,
        };

        Ok(TaskSpec {
            meta,
            poll_config,
            query,
        })
    }
}
