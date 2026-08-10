use std::collections::HashMap;

use chrono::{DateTime, Utc};
use derive_more::Display;
use uuid::Uuid;

use crate::{
    Error,
    error::SnapShotError,
    monitor::taskgroup::{TaskDataUpdateMessage, TaskGroup, TaskGroupId, TaskPosition},
    worker::{TaskEvent, TaskResult, WorkerId},
};

#[derive(Clone, Display)]
pub struct SnapShotId(Uuid);

impl SnapShotId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone, Debug)]
pub struct UpdateTaskState {
    pub payload: TaskDataUpdateMessage,
    pub group_id: TaskGroupId,
    pub task_position: TaskPosition,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    groups: Vec<TaskGroup>,
    last_update: DateTime<Utc>,
}

impl Snapshot {
    pub fn new(groups: Vec<TaskGroup>) -> Self {
        Self {
            groups,
            last_update: Utc::now(),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            groups: Vec::new(),
            last_update: Utc::now(),
        }
    }

    pub fn add_group(&mut self, group: TaskGroup) -> TaskGroupId {
        self.groups.push(group);
        TaskGroupId::new(self.groups.len() - 1)
    }

    pub fn get_mut_taskgroup(&mut self, id: &TaskGroupId) -> Option<&mut TaskGroup> {
        self.groups.get_mut(id.as_usize())
    }

    pub fn update_taskstate(
        &mut self,
        group_id: &TaskGroupId,
        task_position_id: &TaskPosition,
        data: TaskDataUpdateMessage,
    ) -> Result<(), Error> {
        let target = self
            .get_mut_taskgroup(group_id)
            .ok_or(Error::NotFound(format!("Group {} not found", group_id)))?
            .update(task_position_id, data)?;

        Ok(())
    }
}
