use std::{collections::VecDeque, mem};

use crate::polling::{Metrics, PollConfig, PollResult};
use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};

#[derive(Clone, Debug, Copy, Display)]
pub enum Protocol {
    Snmp,
    Http,
    Modbus,
}

#[derive(Clone, Debug, Copy, Display)]
pub enum TypeQuery {
    SnmpGet,
}

#[derive(Clone, Debug, Copy, Display)]
pub enum PollStatus {
    Idle,
    Active,
    Paused,
    RatedLimit,
}

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

#[derive(Clone, Debug)]
pub struct TaskUpdateDto {
    pub snapshot: Option<TaskSnapshot>,
    pub poll_config: Option<PollConfig>,
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
pub struct TaskMeta {
    pub protocol: Protocol,
    pub type_query: TypeQuery,
    pub name: String,
    pub target: String,
    pub subject: String,
}

#[derive(Clone, Debug)]
pub struct TaskSnapshot {
    poll_result: PollResult,
    metrics: Metrics,
    poll_status: PollStatus,
}

impl TaskSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_poll_result(self, poll_result: PollResult) -> Self {
        Self {
            poll_result,
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

    pub fn poll_result(&self) -> &PollResult {
        &self.poll_result
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
            poll_result: PollResult::Initial,
            metrics: Metrics::default(),
        }
    }
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug)]
pub struct TaskEntity {
    id: TaskId,
    meta: TaskMeta,
    snapshot: TaskSnapshot,
    poll_config: PollConfig,
    history: TaskHistory,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskEntity {
    pub fn new(
        id: TaskId,
        meta: TaskMeta,
        snapshot: TaskSnapshot,
        poll_config: PollConfig,
        history: TaskHistory,
    ) -> Self {
        let dt = Local::now();
        Self {
            id,
            meta,
            snapshot,
            poll_config,
            history,
            created_at: dt.clone(),
            updated_at: dt,
        }
    }

    pub fn snapshot(&self) -> &TaskSnapshot {
        &self.snapshot
    }

    pub fn poll_result(&self) -> &PollResult {
        &self.snapshot.poll_result
    }

    pub fn poll_config(&self) -> &PollConfig {
        &self.poll_config
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

    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn history(&self) -> &TaskHistory {
        &self.history
    }

    pub fn update_meta(&mut self, meta: TaskMeta) {
        self.meta = meta;
        self.updated_at = Local::now();
    }

    pub fn update(&mut self, to_update: TaskUpdateDto) -> bool {
        let mut has_update = false;
        let ts = Local::now();
        if let Some(snapshot) = to_update.snapshot {
            let old_snapshot = mem::replace(&mut self.snapshot, snapshot);
            has_update = true;
            self.history.push(HistoryEntry {
                timestamp: ts.clone(),
                snapshot: old_snapshot,
            });
        }
        if let Some(poll_cfg) = to_update.poll_config {
            self.poll_config = poll_cfg;
            has_update = true;
        }

        if has_update {
            self.updated_at = ts;
        }

        has_update
    }
}
