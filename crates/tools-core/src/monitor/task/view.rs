use chrono::{DateTime, Utc};

use crate::monitor::{
    probe::{ProbeExecution, ProbeSpec},
    task::{ExecutionMetrics, SpecRevision, TaskId, TaskStatus, task::Health},
};

#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub name: String,
    pub revision: SpecRevision,
    pub target: String,
    pub status: TaskStatus,
    pub deep_history: u8,
    pub spec: ProbeSpec,
    pub metrics: ExecutionMetrics,
    pub result: Option<ProbeExecution>,
    pub history: Vec<HistoryEntryView>,
    pub health: Health,
}

#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Utc>,
    pub result: ProbeExecution,
}

#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub tasks: Vec<TaskView>,
}
