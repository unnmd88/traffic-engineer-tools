use anyhow::Context;
use serde::Deserialize;
use tools_core::monitor::application::{
    app::Application,
    config::{
        AppConfig, AttemptPollTimingsDto, Query, QuerySnmpGet, SnmpOidItem,
        TaskConfigDto as AppTaskConfigDto,
    },
};

use crate::monitor::queries::snmpget::SnmpGetQueryDto;

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
    limit: u64,
    deep_history: Option<u8>,
    attempt_config: PollTimingsDto,
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
            let attempt_timings = AttemptPollTimingsDto {
                retries: t.attempt_config.retries,
                retry_delay_ms: t.attempt_config.retry_delay_ms,
                timeout_ms: t.attempt_config.timeout_ms,
            };

            let to_query = match &t.query {
                TaskDto::SnmpGet(q) => {
                    let query = QuerySnmpGet {
                        profile: q.profile.clone(),
                        host: q.host.clone(),
                        port: q.port,
                        community: q.community.clone(),
                        oids: q
                            .oids
                            .iter()
                            .map(|item| SnmpOidItem {
                                name: item.name.clone(),
                                oid: item.oid.clone(),
                                value: item.value.clone(),
                            })
                            .collect(),
                    };
                    Query::SnmpGet(query)
                }
            };

            let task_config = AppTaskConfigDto {
                name: t.name.clone(),
                interval_ms: t.interval_seconds * 1000,
                limit: t.limit,
                attempt_timings,
                query: to_query,
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
        let task_states = Vec::with_capacity(raw_tasks.len());

        for (i, task) in raw_tasks.into_iter().enumerate() {}

        Ok(task_states)
    }
}
