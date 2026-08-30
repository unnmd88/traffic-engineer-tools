use std::{
    collections::VecDeque,
    fmt::{self, Formatter},
    fs::OpenOptions,
    mem,
};

use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};
use tokio::task::futures::TaskLocalFuture;

use crate::{
    constants::{DT_FMT, DT_FMT_WITH_MICROSECONDS},
    polling::{Metrics, PollResult},
};
use tracing::{debug, error, info, warn};

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
    Finished,
}

#[derive(Clone, Debug)]
pub struct TaskHistory {
    max: usize,
    history: VecDeque<TaskSnapshot>,
}

impl TaskHistory {
    pub fn new(max_history: u8) -> Self {
        let max_as_usize = max_history as usize;
        Self {
            max: max_as_usize,
            history: VecDeque::with_capacity(max_as_usize),
        }
    }

    pub fn push(&mut self, snapshot: TaskSnapshot) {
        if self.max == 0 {
            return;
        }

        if self.history.len() >= self.max {
            self.history.pop_back();
        }
        self.history.push_front(snapshot);
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskSnapshot> {
        self.history.iter()
    }

    pub fn deep(&self) -> usize {
        self.max
    }

    pub fn len(&self) -> usize {
        self.history.len()
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
pub struct TaskData {
    result: PollResult,
    metrics: Metrics,
}

impl TaskData {
    pub fn new(result: PollResult, metrics: Option<Metrics>) -> Self {
        Self {
            result,
            metrics: metrics.unwrap_or_default(),
        }
    }

    pub fn result(&self) -> &PollResult {
        &self.result
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

impl Default for TaskData {
    fn default() -> Self {
        Self {
            result: PollResult::Initial,
            metrics: Metrics::default(),
        }
    }
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
    history: TaskHistory,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskEntity {
    pub fn new(id: TaskId, meta: TaskMeta, snapshot: TaskSnapshot, history: TaskHistory) -> Self {
        let dt = Local::now();
        Self {
            id,
            meta,
            snapshot,
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

    pub fn update(&mut self, new_snapshot: TaskSnapshot) {
        let old_snapshot = mem::replace(&mut self.snapshot, new_snapshot);
        self.history.push(old_snapshot);
        self.updated_at = Local::now();
    }
}

/*
impl std::fmt::Display for TaskEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let meta = &self.meta;
        writeln!(
            f,
            "Last update: {} Created: {}",
            self.updated_at.format(DT_FMT_WITH_MICROSECONDS),
            self.created_at.format(DT_FMT)
        )?;
        writeln!(f, "Target: {} Name: '{}' Id: {}", meta.target, meta.target, self.id)?;
        writeln!(f, "Subject: {}\n", meta.subject)?;

        let data = &self.data;

        let m = &data.metrics;
        if m.total_attempts > 0 {
            //writeln!(f, "\nMetrics:")?;
            writeln!(
                f,
                "Requests: Total={} Successfull={} Errors={}",
                m.total_attempts, m.successful, m.errors
            )?;
            writeln!(
                f,
                "Latency ms: Current={} Avg={} Min={} Max={}",
                m.current_latency_ms,
                m.avg_latency_ms,
                if m.min_latency_ms == u64::MAX {
                    0
                } else {
                    m.min_latency_ms
                },
                m.max_latency_ms
            )?;
        }

        match &data.result {
            PollResult::SnmpGet(response) => {
                write!(f, "Snmp-get response:\n{response}")?;
            }
            PollResult::NoResponse(errors) => {
                writeln!(f, "Timeout error after {} attempts:", errors.len())?;
                for err in errors.iter() {
                    write!(f, "{err}")?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}
*/
