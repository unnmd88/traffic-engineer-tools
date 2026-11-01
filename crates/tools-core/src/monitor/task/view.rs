use chrono::{DateTime, Local};
use tokio::time::Duration;

use crate::{
    monitor::{
        probe::{ProbeOutput, ProbeSpec},
        task::{SpecRevision, TaskId, TaskStatus, task::Health},
    },
    polling::{AttemptConfig, Metrics, Response},
};

#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub name: String,
    pub revision: SpecRevision,
    pub target: String,
    pub status: TaskStatus,
    pub interval: Duration,
    pub limit: u64,
    pub probe: ProbeSpec,
    pub attempt: AttemptConfig,
    pub deep_history: u8,
    pub metrics: Metrics,
    pub result: Option<Response<ProbeOutput>>,
    pub history: Vec<HistoryEntryView>,
    pub health: Health,
}

#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Local>,
    pub result: Option<Response<ProbeOutput>>,
}

#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub tasks: Vec<TaskView>,
}
