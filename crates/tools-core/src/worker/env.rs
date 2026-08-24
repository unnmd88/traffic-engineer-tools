use derive_more::{Constructor, Display, Into};

use crate::{
    error::PollError,
    worker::{Metrics, task_result::TaskResult},
};

#[derive(Debug, Display, Into, Clone, Copy, Hash, Eq, PartialEq, Constructor)]
pub struct WorkerId(pub u64);

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    Running,
    Stopped,
    Finished,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerCommand {
    Start,
    Stop,
}

#[derive(PartialEq, Eq)]
pub enum WorkerResponse {
    CurrentState(WorkerState),
}

#[derive(Clone)]
pub struct TaskEvent {
    pub worker_id: WorkerId,
    pub task_result: TaskResult,
    pub metrics: Metrics,
    pub worker_state: WorkerState,
}
