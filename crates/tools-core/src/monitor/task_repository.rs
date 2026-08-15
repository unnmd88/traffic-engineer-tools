use core::task;
use std::collections::HashMap;

use crate::{
    Error,
    constants::HUMAN_DT_FMT,
    error::SnapShotError,
    monitor::task::{
        Task, TaskData, TaskDataUpdateMessage, TaskEntity, TaskHistory, TaskId, TaskMeta,
    },
    utils::format_moscow_human,
    worker::{Metrics, TaskEvent, TaskResult, WorkerId},
};
use chrono::{DateTime, Local, Utc};
use constcat::concat;
use derive_more::Display;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct TaskIdGenerator {
    current: usize,
}

impl TaskIdGenerator {
    pub fn new(start_id: usize) -> Self {
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
    order: Vec<TaskId>,
    id_gen: TaskIdGenerator,
    last_update: DateTime<Local>,
}

impl TaskRepository {
    pub fn new(tasks: Vec<TaskEntity>) -> Self {
        let mut order = Vec::new();
        let mut _tasks = HashMap::new();

        for task in tasks {
            order.push(task.id().clone());
            _tasks.insert(task.id().clone(), task);
        }

        let max_id = match order.iter().max() {
            Some(id) => id.as_usize() + 1,
            None => 1,
        };

        Self {
            tasks: _tasks,
            order,
            id_gen: TaskIdGenerator::new(max_id),
            last_update: Local::now(),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            tasks: HashMap::new(),
            order: Vec::new(),
            id_gen: TaskIdGenerator::new(0),
            last_update: Local::now(),
        }
    }

    pub fn add_task(
        &mut self,
        meta: TaskMeta,
        data: Option<TaskData>,
        history: Option<TaskHistory>,
    ) -> TaskId {
        let id = self.id_gen.next();

        let task = TaskEntity::new(
            id.clone(),
            meta,
            data.unwrap_or_default(),
            history.unwrap_or_default(),
        );
        self.tasks.insert(id.clone(), task);
        self.order.push(id.clone());

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

    pub fn get_mut_task(&mut self, id: &TaskId) -> Option<&mut TaskEntity> {
        self.tasks.get_mut(&id)
    }

    pub fn update_taskstate(
        &mut self,
        task_id: &TaskId,
        data: TaskDataUpdateMessage,
    ) -> Result<(), Error> {
        let target = self.get_mut_task(task_id).ok_or_else(|| {
            error!(
            target: "TaskRepository",
            task_id = ?task_id,
            "Task not found"
            );
            Error::NotFound(format!("Task whith id: {task_id} not found"))
        })?;

        target.update_data(TaskData::new(Some(data.task_result), Some(data.metrics)));
        Ok(())
    }
}

use std::fmt::{self, Display, Formatter};

const LINE_THIN: &str =
    "────────────────────────────────────────────────────────────────────────────────";
const LINE_DOUBLE: &str =
    "════════════════════════════════════════════════════════════════════════════";
const LINE_DOTTED: &str =
    "················································································";
const TITLE: &str = "SNAPSHOT";
const SPACES: &str = "                                      ";
const SNAPSHOT_HEADER: &str = concat!(LINE_DOUBLE, "\n", SPACES, TITLE, "\n", LINE_DOUBLE);

impl Display for TaskRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        "TaskRepository Display".to_string();
        Ok(())
    }
}

/*
impl Display for TaskRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // ============================================================
        // Заголовок
        // ============================================================

        writeln!(f, "{SNAPSHOT_HEADER}")?;
        writeln!(f, "Last update: {}", &self.last_update)?;
        writeln!(f, "Total groups: {} Total tasks: {}\n", self.groups.len(), self.total_tasks())?;
        //writeln!(f, "{LINE_THIN}")?;

        // ============================================================
        // Группы
        // ============================================================
        for (group_idx, group) in self.groups.iter().enumerate() {
            writeln!(f, "Task group name: {} ({} tasks)", group.name(), group.len())?;

            for (task_idx, task) in group.tasks().iter().enumerate() {
                // ============================================================
                // Шапка задачи
                // ============================================================

                writeln!(f, "\nTask name: '{}'", task.meta.name)?;

                // ============================================================
                // Метаданные
                // ============================================================
                writeln!(f, "Target: {}", task.meta.target)?;
                writeln!(f, "Subject: {}", task.meta.subject)?;

                // ============================================================
                // Статус и данные
                // ============================================================

                match &task.data.result {
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

                // ============================================================
                // Метрики
                // ============================================================
                let m = &task.data.metrics;
                if m.total_attempts > 0 {
                    //writeln!(f, "\nMetrics:")?;
                    writeln!(
                        f,
                        "Requests| Total: {} Successfull: {} Errors: {}",
                        m.total_attempts, m.successful, m.errors
                    )?;
                    writeln!(
                        f,
                        "Latency ms| Current: {} Avg: {} Min: {} Max: {}",
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

                writeln!(f, "Last update: {}", task.data.last_update.format(HUMAN_DT_FMT))?;
            }
            writeln!(f, "{LINE_DOTTED}")?;
        }

        Ok(())
    }
}
*/
