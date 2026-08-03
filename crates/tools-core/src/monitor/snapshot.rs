use std::collections::HashMap;

use chrono::{DateTime, Utc};
use derive_more::Display;
use uuid::Uuid;

use crate::{
    Error,
    error::SnapShotError,
    monitor::taskgroup::{TaskGroup, TaskGroupId, TaskPosition},
    worker::{TaskEvent, TaskResult, WorkerId},
};

struct WorkerPosition {
    group_id: TaskGroupId,
    position: TaskPosition,
}

#[derive(Clone, Display)]
pub struct SnapShotId(Uuid);

impl SnapShotId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

pub struct Snapshot {
    id: SnapShotId,
    groups: Vec<TaskGroup>,
    worker_mapping: HashMap<WorkerId, WorkerPosition>,
    last_update: DateTime<Utc>,
}

impl Snapshot {
    pub fn new(groups: Vec<TaskGroup>, worker_mapping: HashMap<WorkerId, WorkerPosition>) -> Self {
        Self {
            id: SnapShotId::generate(),
            groups,
            worker_mapping,
            last_update: Utc::now(),
        }
    }

    pub fn update(&mut self, event: TaskEvent) -> Result<(), Error> {
        let location = self.worker_mapping.get(&event.worker_id).ok_or(
            SnapShotError::UpdateWorkerNotFound {
                snapshot_id: self.id.to_string(),
                worker_id: event.worker_id.to_string(),
            },
        )?;

        self.groups
            .get_mut(location.group_id.as_usize())
            .ok_or(Error::NotFound(format!("Group {} not found", location.group_id)))?
            .update(location.position, event)?;

        Ok(())
    }
}
