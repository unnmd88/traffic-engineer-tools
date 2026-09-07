use std::collections::HashMap;

use crate::monitor::task::{PollStatus, TaskEntity, TaskId, TaskSnapshot, TaskSpec};

use super::error::TaskRepositoryError;
use super::view::{MonitorSnapshot, TaskView};
use chrono::{DateTime, Local};
use itertools::Itertools;
use tracing::{error, info, warn};

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

    pub fn add_task(&mut self, spec: TaskSpec) -> TaskId {
        let id = self.id_gen.next();
        let task = TaskEntity::new(id.clone(), spec);

        self.tasks.insert(id.clone(), task);
        self.order_ids.push(id.clone());
        self.updated_at = Local::now();

        info!(
            target: "TaskRepository",
            task_id = ?id,
            task_name = %self.get_task(&id).map_or_else(|| "".to_string(), |t| t.name().to_string()),
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

    pub fn update_snapshot(
        &mut self,
        task_id: &TaskId,
        snapshot: TaskSnapshot,
    ) -> Result<(), TaskRepositoryError> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        if target.update_snapshot(snapshot) {
            self.updated_at = Local::now();
        }

        Ok(())
    }

    pub fn update_spec(
        &mut self,
        task_id: &TaskId,
        spec: TaskSpec,
    ) -> Result<(), TaskRepositoryError> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        if target.update_spec(spec) {
            self.updated_at = Local::now();
        }

        Ok(())
    }

    pub fn update_status(
        &mut self,
        task_id: &TaskId,
        status: PollStatus,
    ) -> Result<(), TaskRepositoryError> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        if target.set_status(status) {
            self.updated_at = Local::now();
        }

        Ok(())
    }

    pub fn reset_metrics(&mut self, task_id: &TaskId) -> Result<(), TaskRepositoryError> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        if target.reset_metrics() {
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

    /// Полный снапшот всех задач в виде read-model (`TaskView`), а не внутренних
    /// `TaskEntity`. Для `get_snapshot` / первичной синхронизации интерфейса.
    pub fn snapshot(&self) -> MonitorSnapshot {
        MonitorSnapshot {
            tasks: self.tasks_sorted_by_id().map(TaskView::from).collect(),
        }
    }

    pub fn created_at(&self) -> &DateTime<Local> {
        &self.created_at
    }
}
