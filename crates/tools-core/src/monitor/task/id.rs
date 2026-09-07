use derive_more::{Constructor, Display};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub enum PollStatus {
    Idle,
    /// Ручной старт: build в процессе, воркера ещё нет.
    Starting,
    Active,
    Paused,
    RatedLimit,
    /// Пересоздание (после падения / update): build или backoff.
    Restarting,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);
