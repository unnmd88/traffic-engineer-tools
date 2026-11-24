use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::monitor::scenario::ScenarioExecution;

use super::{
    ExecutionMetrics, RunLimit, ScenarioFailure, SpecRevision, TaskDescription, TaskId,
    TaskProgress, TaskStatus, TaskTransition,
};

/// An accepted configuration revision captured for observation.
///
/// Acceptance does not imply that a scenario using this revision is ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TaskConfigurationSnapshot {
    /// The task's accepted configuration revision.
    pub revision: SpecRevision,
    /// UTC time at which this revision was accepted.
    pub accepted_at: DateTime<Utc>,
    /// The accepted configuration without transport credentials or parser callbacks.
    pub description: TaskDescription,
}

/// An owned observation of a task's configuration, execution state, and retained results.
///
/// Mutating a snapshot does not change the task in the monitor. Retained results may
/// belong to an earlier configuration revision than the accepted configuration.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    /// The task identifier within this monitor instance.
    pub id: TaskId,
    /// UTC time at which the task was registered.
    pub created_at: DateTime<Utc>,
    /// The latest accepted configuration, independent of execution readiness.
    pub configuration: TaskConfigurationSnapshot,
    /// UTC time of the last accounted activity, not every status or progress change.
    pub updated_at: DateTime<Utc>,
    /// The current task lifecycle status.
    pub status: TaskStatus,
    /// The latest activity of the current preparation; absent outside `Preparing`.
    pub progress: Option<TaskProgress>,
    /// The last actual state change, absent before the first transition.
    /// Its restart plan is present only while the task is waiting for that restart.
    pub last_transition: Option<TaskTransition>,
    /// Execution accounting, reset by an explicit new task start.
    pub metrics: ExecutionMetrics,
    /// The limit fixed at the latest start, absent before the first start.
    pub run_limit: Option<RunLimit>,
    /// Accounted runs since the latest start, including earlier runner instances.
    pub completed_runs: u64,
    /// The latest retained operation result with its original execution context.
    pub result: Option<ScenarioExecution>,
    /// Previous results ordered from newest to oldest, excluding the latest result.
    pub history: Vec<ScenarioExecution>,
    /// The latest scenario failure, retained through restart preparation and pause/stop.
    /// Cleared by an explicit new start, successful preparation, or an accounted result.
    pub last_failure: Option<ScenarioFailure>,
}
