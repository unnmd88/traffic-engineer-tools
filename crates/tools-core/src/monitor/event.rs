use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::monitor::{
    scenario::{
        Generation, PanicStage, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError,
    },
    task::{ExecutionMetrics, SpecRevision, TaskId},
};

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventMeta {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub source: String,
    pub task_id: TaskId,
    pub generation: Generation,
    pub revision: SpecRevision,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum EventData {
    TaskAdded,
    TaskSpecUpdated,
    #[non_exhaustive]
    TaskStarting {
        attempt: u32,
    },
    TaskStarted,
    TaskPaused,
    TaskResumed,
    TaskStopped,
    TaskCompleted,
    TaskRemoved,

    #[non_exhaustive]
    ExecutionFinished {
        execution: ScenarioExecution,
        metrics: ExecutionMetrics,
    },

    #[non_exhaustive]
    BuildFailed {
        error: ScenarioBuildError,
        retry_in: Option<Duration>,
    },
    #[non_exhaustive]
    ExecutionFailed {
        error: ScenarioExecutionError,
        retry_in: Option<Duration>,
    },
    #[non_exhaustive]
    ScenarioPanicked {
        stage: PanicStage,
        reason: String,
        retry_in: Option<Duration>,
    },
}

#[derive(Clone, Debug)]
pub struct MonitorEvent {
    pub metadata: EventMeta,
    pub data: EventData,
}
