use chrono::{DateTime, Local, Utc};
use derive_more::{Constructor, Deref, Display, Eq};

use crate::{
    Error,
    error::ParseError,
    monitor::task::{Task, TaskData},
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
    tasks: Vec<Task>,
    last_update: DateTime<Local>,
}

#[derive(Clone, Debug)]
pub struct TaskDataUpdateMessage {
    pub task_result: TaskResult,
    pub metrics: Metrics,
}

impl TaskGroup {
    pub fn new(name: TaskGroupName, tasks: Vec<Task>) -> Self {
        Self {
            name,
            tasks,
            last_update: Local::now(),
        }
    }

    pub fn new_empty(name: TaskGroupName) -> Self {
        Self {
            name,
            tasks: Vec::new(),
            last_update: Local::now(),
        }
    }

    pub fn name(&self) -> &TaskGroupName {
        &self.name
    }

    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    pub fn last_update(&self) -> DateTime<Local> {
        self.last_update
    }

    pub fn add_taskstate(&mut self, value: Task) -> TaskPosition {
        self.tasks.push(value);
        TaskPosition::new(self.tasks.len() - 1)
    }

    pub fn get_task(&self, task_position: &TaskPosition) -> Option<&Task> {
        self.tasks.get(task_position_to_idx(task_position))
    }

    pub fn get_mut_task(&mut self, task_position: &TaskPosition) -> Option<&mut Task> {
        self.tasks.get_mut(task_position_to_idx(task_position))
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
            last_update: Local::now(),
        };

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }
}

fn task_position_to_idx(task_position: &TaskPosition) -> usize {
    task_position.as_usize()
}
