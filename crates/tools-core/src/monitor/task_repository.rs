use core::task;
use std::collections::HashMap;

use crate::{
    Error,
    constants::{DT_FMT, DT_FMT_WITH_MICROSECONDS},
    error::TaskRepositoryError,
    monitor::task::{
        PollStatus, TaskData, TaskEntity, TaskHistory, TaskId, TaskMeta, TaskPollConfig,
        TaskSnapshot, TaskUpdateDto,
    },
    polling::{Metrics, PollResult},
    utils::format_moscow_human,
};
use chrono::{DateTime, Local};
use constcat::concat;
use derive_more::Display;
use itertools::Itertools;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct TaskIdGenerator {
    current: u64,
}

impl TaskIdGenerator {
    pub fn new(start_id: u64) -> Self {
        Self { current: start_id }
    }

    pub fn next(&mut self) -> TaskId {
        self.current += 1;
        TaskId::new(self.current)
    }
}

#[derive(Clone, Debug)]
pub struct TaskSnapshotUpdate {
    pub poll_result: PollResult,
    pub poll_status: PollStatus,
    pub metrics: Metrics,
}

#[derive(Clone, Debug)]
pub struct TaskRepository {
    tasks: HashMap<TaskId, TaskEntity>,
    id_gen: TaskIdGenerator,
    order_ids: Vec<TaskId>,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskRepository {
    pub fn new(tasks: Vec<TaskEntity>) -> Self {
        let tasks: HashMap<TaskId, TaskEntity> =
            tasks.into_iter().map(|t| (t.id().clone(), t)).collect();

        let max_id = match tasks.keys().max() {
            Some(id) => id.0 + 1,
            None => 1,
        };

        let order_ids: Vec<TaskId> = tasks.keys().copied().sorted().collect();

        let dt = Local::now();

        Self {
            tasks,
            order_ids,
            id_gen: TaskIdGenerator::new(max_id),
            created_at: dt.clone(),
            updated_at: dt,
        }
    }

    pub fn new_empty() -> Self {
        let dt = Local::now();

        Self {
            tasks: HashMap::new(),
            order_ids: Vec::new(),
            id_gen: TaskIdGenerator::new(0),
            created_at: dt.clone(),
            updated_at: dt,
        }
    }

    pub fn add_task(
        &mut self,
        meta: TaskMeta,
        poll_config: Option<TaskPollConfig>,
        task_snapshot: Option<TaskSnapshot>,
        history: Option<TaskHistory>,
    ) -> TaskId {
        let id = self.id_gen.next();
        let task_snaphot = task_snapshot.unwrap_or_else(|| TaskSnapshot::new());
        let poll_config = poll_config.unwrap_or_default();

        let task = TaskEntity::new(
            id.clone(),
            meta,
            task_snaphot,
            poll_config,
            history.unwrap_or_default(),
        );

        self.tasks.insert(id.clone(), task);
        self.order_ids.push(id.clone());
        self.updated_at = Local::now();

        info!(
            target: "TaskRepository",
                task_id = ?id,
                task_name = %self.get_task(&id).map_or_else(|| "".to_string(), |t| t.meta().name.clone()),
                "New task added successfuly."
        );

        id
    }

    pub fn get_task(&self, id: &TaskId) -> Option<&TaskEntity> {
        self.tasks.get(&id)
    }

    fn get_mut_task(&mut self, id: &TaskId) -> Option<&mut TaskEntity> {
        self.tasks.get_mut(&id)
    }

    pub fn update_task(
        &mut self,
        task_id: &TaskId,
        to_update: TaskUpdateDto,
        //snapshot: TaskSnapshotUpdate,
    ) -> Result<(), TaskRepositoryError> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        if target.update(to_update) {
            self.updated_at = Local::now();
        }

        Ok(())
    }

    pub fn remove_task(&mut self, task_id: &TaskId) -> Result<TaskEntity, TaskRepositoryError> {
        let removed_task = match self.tasks.remove(task_id) {
            Some(task) => task,
            None => {
                warn!(
                    target: "TaskRepository",
                    task_id = ?task_id,
                    "Attempted to remove non-existent task"
                );
                return Err(TaskRepositoryError::TaskNotFound {
                    task_id: task_id.to_string(),
                });
            }
        };
        self.order_ids.retain(|id| id != removed_task.id());

        info!(
            target: "TaskRepository",
            task_id = ?task_id,
            "Task removed successfully"
        );
        self.updated_at = Local::now();

        Ok(removed_task)
    }

    pub fn sorted_task_ids(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.order_ids.iter().copied()
    }

    pub fn tasks_sorted_by_id(&self) -> impl Iterator<Item = &TaskEntity> + '_ {
        self.order_ids
            .iter()
            .filter_map(|id| match self.tasks.get(id) {
                Some(task) => Some(task),
                None => {
                    error!(
                        target: "task_repository",
                            task_id = ?id,
                            "TaskId has in order, but not found in `tasks`"
                    );
                    None
                }
            })
    }

    pub fn tasks(&self) -> impl Iterator<Item = &TaskEntity> + '_ {
        self.tasks.values()
    }

    pub fn created_at(&self) -> &DateTime<Local> {
        &self.created_at
    }
}
