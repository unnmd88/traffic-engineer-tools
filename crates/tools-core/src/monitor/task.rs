use std::{
    collections::VecDeque,
    fmt::{self, Formatter},
    mem,
};

use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};

use crate::{
    constants::{DT_FMT, DT_FMT_WITH_MICROSECONDS},
    worker::{Metrics, TaskResult},
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

#[derive(Clone, Debug)]
pub struct TaskHistory {
    max: usize,
    history: VecDeque<TaskData>,
}

impl TaskHistory {
    pub fn new(max_history: u8) -> Self {
        let max_as_usize = max_history as usize;
        Self {
            max: max_as_usize,
            history: VecDeque::with_capacity(max_as_usize),
        }
    }

    pub fn push(&mut self, task_data: TaskData) {
        if self.max == 0 {
            return;
        }

        if self.history.len() >= self.max {
            self.history.pop_back();
        }
        self.history.push_front(task_data);
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskData> {
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
    result: TaskResult,
    metrics: Metrics,
    //pub last_update: DateTime<Local>,
}

impl TaskData {
    pub fn new(result: Option<TaskResult>, metrics: Option<Metrics>) -> Self {
        Self {
            result: result.unwrap_or_else(|| TaskResult::Initial),
            metrics: metrics.unwrap_or_default(),
            //last_update: Local::now(),
        }
    }

    pub fn result(&self) -> &TaskResult {
        &self.result
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

impl Default for TaskData {
    fn default() -> Self {
        Self {
            result: TaskResult::Initial,
            metrics: Metrics::default(),
            //last_update: Local::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskState {
    pub data: TaskData,
    pub history: TaskHistory,
}

#[derive(Clone, Debug)]
pub struct Task {
    pub meta: TaskMeta,
    pub data: TaskData,
    pub history: TaskHistory,
}

#[derive(Clone, Debug)]
pub struct TaskDataUpdateMessage {
    pub task_result: TaskResult,
    pub metrics: Metrics,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug)]
pub struct TaskEntity {
    id: TaskId,
    meta: TaskMeta,
    data: TaskData,
    history: TaskHistory,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskEntity {
    pub fn new(id: TaskId, meta: TaskMeta, data: TaskData, history: TaskHistory) -> Self {
        let dt = Local::now();
        Self {
            id,
            meta,
            data,
            history,
            created_at: dt.clone(),
            updated_at: dt,
        }
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

    pub fn data(&self) -> &TaskData {
        &self.data
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn update_meta(&mut self, meta: TaskMeta) {
        self.meta = meta;
        self.updated_at = Local::now();
    }

    pub fn update_data(&mut self, data: TaskData) {
        let old_data = mem::replace(&mut self.data, data);
        self.history.push(old_data);
        self.updated_at = Local::now();
    }

    pub fn history(&self) -> &TaskHistory {
        &self.history
    }
}

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
            TaskResult::SnmpGet(response) => {
                writeln!(f, "Snmp-get response:\n{response}")?;
            }
            TaskResult::NoResponse(errors) => {
                writeln!(f, "Timeout error after {} attempts:", errors.len())?;
                for err in errors.iter() {
                    writeln!(f, "{err}")?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}
