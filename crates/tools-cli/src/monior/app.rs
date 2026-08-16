use std::net::IpAddr;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tools_core::{
    Error,
    monitor::application::{
        app::Application,
        config::{AppConfig, Query, QuerySnmpGet, SnmpOidItem, TaskConfig, TaskPollTimings},
    },
    snmp::{SnmpQueryItem, primitives::Community},
};

use crate::monior::queries::snmpget::SnmpGetQueryDto;

#[derive(Debug, Deserialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
pub enum TaskDto {
    SnmpGet(SnmpGetQueryDto),
}

#[derive(Debug, Deserialize)]
struct PollTimingsDto {
    timeout_ms: u64,
    retries: u8,
    retry_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
struct TaskConfigDto {
    name: String,
    interval_seconds: u64,
    deep_history: u8,
    poll_timings: PollTimingsDto,
    query: TaskDto,
}

#[derive(Debug, Deserialize)]
pub struct MonitorConfigDto {
    tasks: Vec<TaskConfigDto>,
}

pub struct AppBuilder;

impl AppBuilder {
    pub async fn from_yaml(content: &str) -> anyhow::Result<Application> {
        let dto_config: MonitorConfigDto = serde_yaml::from_str(content)?;

        let mut tasks = Vec::new();
        //println!("{:#?}", dto_config);

        for t in dto_config.tasks.iter() {
            let poll_timings = TaskPollTimings {
                retries: t.poll_timings.retries,
                retry_delay_ms: t.poll_timings.retry_delay_ms,
                timeout_ms: t.poll_timings.timeout_ms,
            };

            let to_query = match &t.query {
                TaskDto::SnmpGet(q) => {
                    let query = QuerySnmpGet {
                        host: q.host.clone(),
                        port: q.port,
                        community: q.community.clone(),
                        oids: q
                            .oids
                            .iter()
                            .map(|item| SnmpOidItem {
                                name: item.name.clone(),
                                oid: item.oid.clone(),
                            })
                            .collect(),
                    };
                    Query::SnmpGet(query)
                }
            };

            let task_config = TaskConfig {
                name: t.name.clone(),
                poll_timings,
                query: to_query,
                interval_ms: t.interval_seconds * 1000,
                deep_history: t.deep_history,
            };

            tasks.push(task_config);
        }

        let app_cfg = AppConfig { tasks };
        let app = Application::new(app_cfg).await?;

        Ok(app)

        //Err(Error::Internal("Test".to_string())).context("Заглушка")?
        //parse_config(content)
    }

    //fn build(config: MonitorConfig) -> anyhow::Result<Snapshot> {}
    fn build_tasks(raw_tasks: Vec<TaskDto>, group_id: usize) -> anyhow::Result<Vec<TaskDto>> {
        let mut task_states = Vec::with_capacity(raw_tasks.len());

        for (i, task) in raw_tasks.into_iter().enumerate() {}

        Ok(task_states)
    }
}
