use chrono::{DateTime, Local};

use crate::{
    monitor::{
        task::{TaskEntity, TaskId, TaskRevision, TaskSpec, TaskStatus, TaskView},
        usecase::UseCaseOutput,
    },
    polling::{Metrics, Response},
};

#[derive(Clone, Debug)]
pub struct TaskEvent {
    pub timestamp: DateTime<Local>,
    pub task_view: TaskView,
    pub data: TaskEventData,
}

impl TaskEvent {
    pub fn new(task_view: TaskView, data: TaskEventData) -> Self {
        Self {
            task_view,
            timestamp: Local::now(),
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TaskEventData {
    TaskAdded { spec: TaskSpec },
    TaskSpecUpdated { spec: TaskSpec },
    TaskRemoved,
    TaskStarted,
    TaskStopped,
    TaskRestarting,
    TaskBuildFailed { reason: String },
    TaskCompleted,
    TaskPolled,
}
