use derive_more::{Constructor, Display, Into};

use crate::polling::{Metrics, PollResult};

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
    Finish,
    Stop,
}

#[derive(PartialEq, Eq)]
pub enum WorkerResponse {
    CurrentState(WorkerState),
}

#[derive(Clone)]
pub struct WorkerEvent {
    pub id: WorkerId,
    pub state: WorkerState,
    pub metrics: Metrics,
    pub poll_result: PollResult,
}
