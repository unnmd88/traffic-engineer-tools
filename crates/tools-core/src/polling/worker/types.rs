use derive_more::{Constructor, Display, Into};

use crate::polling::{Metrics, Response};

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

// Периодическое событие воркера: штатный итог опроса (value).
#[derive(Clone)]
pub struct WorkerEvent<T> {
    pub id: WorkerId,
    pub metrics: Metrics,
    pub result: Response<T>,
}

// Финальный итог завершения воркера (через JoinHandle).
#[derive(Debug)]
pub enum WorkerFinished {
    /// Дошёл до лимита / потребитель ушёл.
    Completed,
    /// Фатальная ошибка.
    Failed(String),
}
