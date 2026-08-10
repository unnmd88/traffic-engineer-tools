use std::net::IpAddr;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tools_core::{
    Error,
    monitor::{Snapshot, task::TaskState},
    snmp::{SnmpQueryItem, primitives::Community},
};

use crate::monior::tasks::snmpget::{TaskSnmpGet, TaskSnmpGetDto};

#[derive(Debug, Deserialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
pub enum Task {
    SnmpGet(TaskSnmpGetDto),
}

#[derive(Debug, Deserialize)]
pub struct GroupConfig {
    name: String,
    tasks: Vec<Task>,
}

#[derive(Debug, Deserialize)]
pub struct MonitorConfig {
    groups: Vec<GroupConfig>,
}

pub struct SnapshotBuilder;

impl SnapshotBuilder {
    pub fn from_yaml(content: &str) -> anyhow::Result<Snapshot> {
        let config: MonitorConfig = serde_yaml::from_str(content)?;
        println!("{:#?}", config);
        Err(Error::Internal("Test".to_string())).context("Заглушка")?
        //parse_config(content)
    }

    //fn build(config: MonitorConfig) -> anyhow::Result<Snapshot> {}
    fn build_tasks(raw_tasks: Vec<Task>, group_id: usize) -> anyhow::Result<Vec<TaskState>> {
        let mut task_states = Vec::with_capacity(raw_tasks.len());

        for (i, task) in raw_tasks.into_iter().enumerate() {}

        Ok(task_states)
    }
}
