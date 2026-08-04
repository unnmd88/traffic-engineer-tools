use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use derive_more::{Deref, Display, Eq};

use crate::{
    Error,
    error::ParseError,
    worker::{Metrics, TaskEvent, TaskResult, WorkerId, WorkerState},
};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub struct TaskGroupId(usize);

impl TaskGroupId {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub struct TaskPosition(usize);

impl TaskPosition {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Display)]
pub struct TaskGroupName(String);

impl TaskGroupName {
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 64;

    pub fn from_string(name: &str) -> Result<Self, Error> {
        let len = name.len();

        if !(Self::MIN_LEN..=Self::MAX_LEN).contains(&len) {
            let message = match len {
                0 => "value is empty".to_string(),
                l if l < Self::MIN_LEN => {
                    format!("too short (got {l}, need at least {})", Self::MIN_LEN)
                }
                _ => format!("too long (got {len}, max {})", Self::MAX_LEN),
            };

            return Err(Error::Parse(ParseError::InvalidLength {
                message,
                min: Self::MIN_LEN,
                max: Self::MAX_LEN,
                provided: len,
            }));
        }

        Ok(Self(name.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct TaskMeta {
    pub name: String,
    pub target: String,
    pub subject: String,
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
}

#[derive(Clone, Debug)]
pub struct TaskData {
    pub result: TaskResult,
    pub metrics: Metrics,
    pub last_update: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TaskState {
    pub meta: TaskMeta,
    pub data: TaskData,
    //pub worker_state: WorkerState,
    pub history: TaskHistory,
}

#[derive(Clone, Debug)]
pub struct TaskGroup {
    id: TaskGroupId,
    name: TaskGroupName,
    tasks: Vec<TaskState>,
    last_update: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TaskDataUpdateMessage {
    pub task_result: TaskResult,
    pub metrics: Metrics,
}

impl TaskGroup {
    pub fn new(id: TaskGroupId, name: TaskGroupName, tasks: Vec<TaskState>) -> Self {
        Self {
            id,
            name,
            tasks,
            last_update: Utc::now(),
        }
    }

    pub fn update(
        &mut self,
        task_position: TaskPosition,
        payload: TaskDataUpdateMessage,
    ) -> Result<(), Error> {
        let idx = task_position.as_usize();

        let mut task_state = self
            .tasks
            .get_mut(idx)
            .ok_or(Error::NotFound(format!("Task at position {} not found", idx)))?;
        let old_task = task_state.data.clone();
        task_state.history.push(old_task);

        task_state.data = TaskData {
            result: payload.task_result,
            metrics: payload.metrics,
            last_update: Utc::now(),
        };

        //task_state.worker_state = event.worker_state;

        Ok(())
    }

    pub fn id(&self) -> TaskGroupId {
        self.id.clone()
    }

    pub fn name(&self) -> TaskGroupName {
        self.name.clone()
    }

    pub fn tasks(&self) -> &[TaskState] {
        &self.tasks
    }

    pub fn last_update(&self) -> DateTime<Utc> {
        self.last_update.clone()
    }
}
