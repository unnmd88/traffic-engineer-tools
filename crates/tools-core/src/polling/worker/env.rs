use derive_more::{Constructor, Display, Into};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::polling::{Metrics, PollResult, config::PollConfig};

#[derive(Debug, Display, Into, Clone, Copy, Hash, Eq, PartialEq, Constructor)]
pub struct WorkerId(pub u64);

#[derive(Debug)]
pub struct WorkerHandle {
    mailbox: mpsc::Sender<WorkerCommand>,
    join_handle: JoinHandle<()>,
}

impl WorkerHandle {
    pub fn new(mailbox: mpsc::Sender<WorkerCommand>, join_handle: JoinHandle<()>) -> Self {
        Self {
            mailbox,
            join_handle,
        }
    }
    pub async fn send(
        &self,
        cmd: WorkerCommand,
    ) -> Result<(), mpsc::error::SendError<WorkerCommand>> {
        self.mailbox.send(cmd).await
    }

    pub fn abort(&self) {
        self.join_handle.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.join_handle.is_finished()
    }
}

#[derive(Display, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    Running,
    Stopped,
    RatedLimit,
}

#[derive(Debug, PartialEq, Eq, Display)]
pub enum WorkerCommand {
    Start,
    SetLimit(u64),
    Resume,
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
    pub poll_config: PollConfig,
    pub metrics: Metrics,
    pub poll_result: PollResult,
}
