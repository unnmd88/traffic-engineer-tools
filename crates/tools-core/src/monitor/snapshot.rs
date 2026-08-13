use std::collections::HashMap;

use chrono::{DateTime, Local, Utc};
use derive_more::Display;
use uuid::Uuid;

use crate::{
    Error,
    constants::HUMAN_DT_FMT,
    error::SnapShotError,
    monitor::taskgroup::{TaskDataUpdateMessage, TaskGroup, TaskGroupId, TaskPosition},
    utils::format_moscow_human,
    worker::{Metrics, TaskEvent, TaskResult, WorkerId},
};
use constcat::concat;

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
    last_update: DateTime<Local>,
}

impl Snapshot {
    pub fn new(groups: Vec<TaskGroup>) -> Self {
        Self {
            groups,
            last_update: Local::now(),
        }
    }

    pub fn groups(&self) -> &[TaskGroup] {
        &self.groups
    }

    pub fn total_tasks(&self) -> usize {
        self.groups.iter().map(|g| g.len()).sum()
    }

    pub fn new_empty() -> Self {
        Self {
            groups: Vec::new(),
            last_update: Local::now(),
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
        self.last_update = Local::now();

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

impl Display for Snapshot {
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
            //writeln!(f, "{}", "─".repeat(80))?;

            for (task_idx, task) in group.tasks().iter().enumerate() {
                // ============================================================
                // Шапка задачи
                // ============================================================

                writeln!(f, "\nTask name: '{}'", task.meta.name)?;
                //writeln!(f, "  {}", "·".repeat(50))?;

                // ============================================================
                // Метаданные
                // ============================================================
                writeln!(f, "Target: {}", task.meta.target)?;
                //writeln!(f, " Protocol: {:?}", task.meta.protocol)?;
                //writeln!(f, " Type: {:?}", task.meta.type_query)?;
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
