use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::monitor::{
    ApplicationId, TaskTransition,
    scenario::{PreparationView, ScenarioExecution, ScenarioSpec},
    task::{ExecutionMetrics, SpecRevision, TaskId, TaskStatus, task::Health},
};

/// A scenario's human-readable preparation activity, independent of lifecycle transitions.
///
/// Messages are for display, may change, and must not be parsed as stable identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgress {
    /// UTC time at which the scenario reported this activity.
    pub at: DateTime<Utc>,
    /// The current activity without credentials or other secrets.
    pub message: String,
}

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
    /// The latest activity of the current preparation; absent outside `Preparing`.
    pub progress: Option<TaskProgress>,
    /// The last actual state change, absent before the first transition.
    /// Its restart plan is present only while the task is waiting for that restart.
    pub last_transition: Option<TaskTransition>,
    pub deep_history: u8,
    pub scenario: ScenarioSpec,
    pub limit: u64,
    pub metrics: ExecutionMetrics,
    /// Detailed evidence from the latest preparation run; use `progress` for current activity.
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
