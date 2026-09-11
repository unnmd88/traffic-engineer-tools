use derive_more::{Add, AddAssign, Constructor, Display};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub enum TaskStatus {
    Idle,
    /// Ручной старт: build в процессе, воркера ещё нет.
    Starting,
    Active,
    Stopped,
    RatedLimit,
    /// Пересоздание (после падения / update): build или backoff.
    Restarting,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor)]
pub struct SpecRevision(pub u64);

impl SpecRevision {
    pub fn next(self) -> Self {
        Self::new(self.0 + 1)
    }
}
