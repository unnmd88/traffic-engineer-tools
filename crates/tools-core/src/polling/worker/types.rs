use derive_more::{Constructor, Display, Into};

use crate::polling::{Metrics, PollResult};

#[derive(Debug, Display, Into, Clone, Copy, Hash, Eq, PartialEq, Constructor)]
pub struct WorkerId(pub u64);

#[derive(Debug)]
pub struct WorkerHandle {
    abort: tokio::task::AbortHandle,
}

impl WorkerHandle {
    pub fn new(abort: tokio::task::AbortHandle) -> Self {
        Self { abort }
    }

    pub fn abort(&self) {
        self.abort.abort();
    }

    pub fn id(&self) -> tokio::task::Id {
        self.abort.id()
    }
}

#[derive(Clone)]
pub struct WorkerEvent {
    pub id: WorkerId,
    pub metrics: Metrics,
    pub poll_result: PollResult,
}
