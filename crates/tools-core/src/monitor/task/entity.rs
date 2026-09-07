use std::{collections::VecDeque, mem};

use crate::{
    monitor::{
        task::{PollStatus, TaskId, TaskSpec, UseCaseQuery},
        usecase::UseCaseOutput,
    },
    polling::{Metrics, PollConfig, Response},
};
use chrono::{DateTime, Local};

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Local>,
    pub snapshot: TaskSnapshot,
}

#[derive(Clone, Debug)]
pub struct TaskHistory {
    max: usize,
    history: VecDeque<HistoryEntry>,
}

impl TaskHistory {
    pub fn new(max_history: u8) -> Self {
        let max_as_usize = max_history as usize;
        Self {
            max: max_as_usize,
            history: VecDeque::with_capacity(max_as_usize),
        }
    }

    pub fn push(&mut self, snapshot: HistoryEntry) {
        if self.max == 0 {
            return;
        }

        if self.history.len() >= self.max {
            self.history.pop_back();
        }
        self.history.push_front(snapshot);
    }

    pub fn iter(&self) -> impl Iterator<Item = &HistoryEntry> {
        self.history.iter()
    }

    pub fn deep(&self) -> usize {
        self.max
    }

    pub fn len(&self) -> usize {
        self.history.len()
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }
}

impl Default for TaskHistory {
    fn default() -> Self {
        Self::new(3)
    }
}

#[derive(Clone, Debug)]
pub struct TaskSnapshot {
    poll_result: Option<Response<UseCaseOutput>>,
    metrics: Metrics,
    poll_status: PollStatus,
}

impl TaskSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_poll_result(self, poll_result: Response<UseCaseOutput>) -> Self {
        Self {
            poll_result: Some(poll_result),
            ..self
        }
    }

    pub fn with_poll_status(self, poll_status: PollStatus) -> Self {
        Self {
            poll_status,
            ..self
        }
    }

    pub fn with_metrics(self, metrics: Metrics) -> Self {
        Self { metrics, ..self }
    }

    pub fn poll_result(&self) -> Option<&Response<UseCaseOutput>> {
        self.poll_result.as_ref()
    }

    pub fn poll_status(&self) -> &PollStatus {
        &self.poll_status
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

impl Default for TaskSnapshot {
    fn default() -> Self {
        Self {
            poll_status: PollStatus::Idle,
            poll_result: None,
            metrics: Metrics::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskEntity {
    id: TaskId,
    spec: TaskSpec,
    snapshot: TaskSnapshot,
    history: TaskHistory,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskEntity {
    pub fn new(id: TaskId, spec: TaskSpec) -> Self {
        let dt = Local::now();
        Self {
            id,
            snapshot: TaskSnapshot::default(),
            history: TaskHistory::new(spec.deep_history()),
            spec,
            created_at: dt.clone(),
            updated_at: dt,
        }
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    pub fn name(&self) -> &str {
        self.spec.name()
    }

    pub fn query(&self) -> &UseCaseQuery {
        self.spec.query()
    }

    pub fn poll_config(&self) -> &PollConfig {
        self.spec.poll_config()
    }

    pub fn snapshot(&self) -> &TaskSnapshot {
        &self.snapshot
    }

    pub fn poll_result(&self) -> Option<&Response<UseCaseOutput>> {
        self.snapshot.poll_result()
    }

    pub fn status(&self) -> &PollStatus {
        &self.snapshot.poll_status
    }

    pub fn metrics(&self) -> &Metrics {
        &self.snapshot.metrics
    }

    pub fn created_at(&self) -> &DateTime<Local> {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTime<Local> {
        &self.updated_at
    }

    pub fn history(&self) -> &TaskHistory {
        &self.history
    }

    pub fn update_snapshot(&mut self, snapshot: TaskSnapshot) -> bool {
        let ts = Local::now();
        let old_snapshot = mem::replace(&mut self.snapshot, snapshot);
        self.history.push(HistoryEntry {
            timestamp: ts.clone(),
            snapshot: old_snapshot,
        });
        self.updated_at = ts;
        true
    }

    pub fn update_spec(&mut self, spec: TaskSpec) -> bool {
        self.spec = spec;
        self.updated_at = Local::now();
        true
    }

    pub fn set_status(&mut self, status: PollStatus) -> bool {
        self.snapshot.poll_status = status;
        self.updated_at = Local::now();
        true
    }

    /// Сбросить счётчики метрик — начало нового запуска (ручной `start`).
    pub fn reset_metrics(&mut self) -> bool {
        self.snapshot.metrics = Metrics::default();
        self.updated_at = Local::now();
        true
    }
}
