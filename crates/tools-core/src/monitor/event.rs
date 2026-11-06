use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::monitor::{
    probe::{ProbeError, ProbeExecution},
    scenario::Generation,
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
    TaskStarting {
        attempt: u32,
    },
    TaskStarted,
    TaskPaused,
    TaskResumed,
    TaskStopped,
    TaskCompleted,
    TaskRemoved,

    ProbeExecuted {
        execution: ProbeExecution,
        metrics: ExecutionMetrics,
    },

    BuildFailed {
        reason: String,
        retry_in: Option<Duration>,
    },
    ProbeError {
        error: ProbeError,
        retry_in: Option<Duration>,
    },
    WorkerPanicked {
        reason: String,
        retry_in: Option<Duration>,
    },
}

#[derive(Clone, Debug)]
pub struct MonitorEvent {
    pub metadata: EventMeta,
    pub data: EventData,
}
