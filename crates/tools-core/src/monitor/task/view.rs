use chrono::{DateTime, Utc};

use crate::monitor::{
    ApplicationId, TaskTransition,
    scenario::{PreparationView, ScenarioExecution, ScenarioSpec},
    task::{ExecutionMetrics, SpecRevision, TaskId, TaskStatus, task::Health},
};

#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub name: String,
    pub revision: SpecRevision,
    pub created_at: DateTime<Utc>,
    pub config_updated_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub target: String,
    pub status: TaskStatus,
    /// The last actual state change, absent before the first transition.
    /// Its restart plan is present only while the task is waiting for that restart.
    pub last_transition: Option<TaskTransition>,
    pub deep_history: u8,
    pub scenario: ScenarioSpec,
    pub limit: u64,
    pub metrics: ExecutionMetrics,
    /// The latest preparation run, including its current stage and terminal report.
    pub preparation: Option<PreparationView>,
    pub result: Option<ScenarioExecution>,
    pub history: Vec<HistoryEntryView>,
    pub health: Health,
}

#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Utc>,
    pub result: ScenarioExecution,
}

#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub application_id: ApplicationId,
    pub next_seq: u64,
    pub tasks: Vec<TaskView>,
}
