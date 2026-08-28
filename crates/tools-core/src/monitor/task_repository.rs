use core::task;
use std::collections::HashMap;

use crate::{
    Error,
    constants::{DT_FMT, DT_FMT_WITH_MICROSECONDS},
    error::TaskRepositoryError,
    monitor::task::{Task, TaskData, TaskEntity, TaskHistory, TaskId, TaskMeta},
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
pub struct TaskDataUpdate {
    pub poll_result: PollResult,
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
        data: Option<TaskData>,
        history: Option<TaskHistory>,
    ) -> TaskId {
        let id = self.id_gen.next();
        let data = data.unwrap_or_else(|| TaskData::new(PollResult::Initial, None));

        let task = TaskEntity::new(id.clone(), meta, data, history.unwrap_or_default());

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

    pub fn update_taskstate(
        &mut self,
        task_id: &TaskId,
        data: TaskDataUpdate,
    ) -> Result<(), Error> {
        let target = self
            .get_mut_task(task_id)
            .ok_or(TaskRepositoryError::TaskNotFound {
                task_id: task_id.0.to_string(),
            })?;

        target.update_data(TaskData::new(data.poll_result, Some(data.metrics)));
        self.updated_at = Local::now();

        Ok(())
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
                        target: "task repository",
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
}

use std::fmt::{self, Display, Formatter};

const LINE_THIN: &str =
    "────────────────────────────────────────────────────────────────────────────────";
const LINE_DOUBLE: &str =
    "════════════════════════════════════════════════════════════════════════════";
const LINE_DOTTED: &str =
    "················································································";
const TITLE: &str = "Monitor created at";
const SPACES: &str = "                                      ";
const SNAPSHOT_HEADER: &str = concat!(LINE_DOUBLE, "\n", SPACES, TITLE, "\n", LINE_DOUBLE);

impl Display for TaskRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{LINE_DOUBLE}")?;
        writeln!(
            f,
            "{TITLE}: {}",
            //self.updated_at.format(DT_FMT_WITH_MICROSECONDS),
            self.created_at.format(DT_FMT)
        )?;
        writeln!(f, "{LINE_DOUBLE}")?;

        for task in self.tasks_sorted_by_id() {
            let meta = task.meta();
            let data = task.data();

            writeln!(
                f,
                "Name: '{}' Target: {} Id: {} Created: {}",
                meta.name,
                meta.target,
                task.id(),
                task.created_at().format(DT_FMT)
            )?;
            writeln!(f, "{}\n{LINE_THIN}\n", meta.subject)?;
            writeln!(f, "Last update: {}", task.updated_at().format(DT_FMT_WITH_MICROSECONDS),)?;

            let m = data.metrics();
            if m.total_attempts > 0 {
                writeln!(
                    f,
                    "Requests: Total={} Successfull={} Errors={}",
                    m.total_attempts, m.successful, m.errors
                )?;
                writeln!(
                    f,
                    "Latency ms: Current={} Avg={} Min={} Max={}",
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

            match &task.data().result() {
                PollResult::SnmpGet(response) => {
                    writeln!(f, "Snmp-get response:\n{response}")?;
                }
                PollResult::NoResponse(errors) => {
                    writeln!(f, "Timeout error after {} attempts:", errors.len())?;
                    for err in errors.iter() {
                        writeln!(f, "{err}")?;
                    }
                }
                _ => {}
            }
            writeln!(f, "{LINE_DOTTED}")?;
        }

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
