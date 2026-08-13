use std::collections::VecDeque;

use chrono::{DateTime, Local};
use derive_more::Display;

use crate::worker::{Metrics, TaskResult};

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
        if self.history.len() >= self.max {
            self.history.pop_back();
        }
        self.history.push_front(task_data);
    }

    pub fn deep(&self) -> usize {
        self.max
    }

    pub fn history(&self) -> &VecDeque<TaskData> {
        &self.history
    }

    pub fn len(&self) -> usize {
        self.history.len()
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
    pub result: TaskResult,
    pub metrics: Metrics,
    pub last_update: DateTime<Local>,
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
