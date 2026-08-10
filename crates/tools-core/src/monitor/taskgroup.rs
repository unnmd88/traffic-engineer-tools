use chrono::{DateTime, Utc};
use derive_more::{Constructor, Deref, Display, Eq};

use crate::{
    Error,
    error::ParseError,
    monitor::task::{TaskData, UseCase},
    worker::{Metrics, TaskEvent, TaskResult, WorkerId, WorkerState},
};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor)]
pub struct TaskGroupId(usize);

impl TaskGroupId {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor)]
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

    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let len = name.len();

        if !(Self::MIN_LEN..=Self::MAX_LEN).contains(&len) {
            let message = match len {
                0 => {
                    return Err(ParseError::CantBeEmpty {
                        name: "GroupName".to_string(),
                    });
                }
                l if l < Self::MIN_LEN => {
                    format!("too short (got {l}, need at least {})", Self::MIN_LEN)
                }
                _ => format!("too long (got {len}, max {})", Self::MAX_LEN),
            };

            return Err(ParseError::InvalidLength {
                message,
                min: Self::MIN_LEN,
                max: Self::MAX_LEN,
                provide: len,
            });
        }

        Ok(Self(name.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct TaskGroup {
    name: TaskGroupName,
    tasks: Vec<UseCase>,
    last_update: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TaskDataUpdateMessage {
    pub task_result: TaskResult,
    pub metrics: Metrics,
}

impl TaskGroup {
    pub fn new(name: TaskGroupName, tasks: Vec<UseCase>) -> Self {
        Self {
            name,
            tasks,
            last_update: Utc::now(),
        }
    }

    pub fn new_empty(name: TaskGroupName) -> Self {
        Self {
            name,
            tasks: Vec::new(),
            last_update: Utc::now(),
        }
    }

    pub fn add_taskstate(&mut self, value: UseCase) -> TaskPosition {
        self.tasks.push(value);
        TaskPosition::new(self.tasks.len() - 1)
    }

    pub fn update(
        &mut self,
        task_position: &TaskPosition,
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

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn name(&self) -> TaskGroupName {
        self.name.clone()
    }

    pub fn tasks(&self) -> &[UseCase] {
        &self.tasks
    }

    pub fn last_update(&self) -> DateTime<Utc> {
        self.last_update.clone()
    }
}
