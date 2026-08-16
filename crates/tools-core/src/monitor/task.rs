use std::{collections::VecDeque, mem};

use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};

use crate::worker::{Metrics, TaskResult};
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
    pub last_update: DateTime<Local>,
}

impl TaskData {
    pub fn new(result: Option<TaskResult>, metrics: Option<Metrics>) -> Self {
        Self {
            result: result.unwrap_or_else(|| TaskResult::Initial),
            metrics: metrics.unwrap_or_default(),
            last_update: Local::now(),
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
            last_update: Local::now(),
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
}

impl TaskEntity {
    pub fn new(id: TaskId, meta: TaskMeta, data: TaskData, history: TaskHistory) -> Self {
        Self {
            id,
            meta,
            data,
            history,
        }
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
    }

    pub fn update_data(&mut self, data: TaskData) {
        let old_data = mem::replace(&mut self.data, data);
        self.history.push(old_data);
    }

    pub fn history(&self) -> &TaskHistory {
        &self.history
    }
}
