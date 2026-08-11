use std::collections::HashMap;

use chrono::{DateTime, Utc};
use derive_more::Display;
use uuid::Uuid;

use crate::{
    Error,
    error::SnapShotError,
    monitor::taskgroup::{TaskDataUpdateMessage, TaskGroup, TaskGroupId, TaskPosition},
    utils::format_moscow_human,
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

    pub fn groups(&self) -> &[TaskGroup] {
        &self.groups
    }

    pub fn total_tasks(&self) -> usize {
        self.groups.iter().map(|g| g.len()).sum()
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
        self.last_update = Utc::now();

        Ok(())
    }
}

use std::fmt::{self, Display, Formatter};

impl Display for Snapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // ============================================================
        // Заголовок
        // ============================================================
        writeln!(f, "{}", "═".repeat(60))?;
        writeln!(f, " SNAPSHOT")?;
        writeln!(f, "{}", "═".repeat(60))?;
        writeln!(f, "Last update: {}", format_moscow_human(&self.last_update))?;
        //let total_tasks: usize = self.groups.iter().map(|g| g.len()).sum();
        writeln!(f, "Total groups: {} Total tasks: {}", self.groups.len(), self.total_tasks())?;
        writeln!(f, "{}", "─".repeat(60))?;

        // ============================================================
        // Группы
        // ============================================================
        for (group_idx, group) in self.groups.iter().enumerate() {
            writeln!(f, " Group {} ({} tasks)", group.name(), group.len())?;
            writeln!(f, "{}", "─".repeat(60))?;

            for (task_idx, task) in group.tasks().iter().enumerate() {
                // ============================================================
                // Шапка задачи
                // ============================================================

                writeln!(f, "\n Task  '{}'", task.meta.name)?;
                //writeln!(f, "  {}", "·".repeat(50))?;

                // ============================================================
                // Метаданные
                // ============================================================
                writeln!(f, " Target: {}", task.meta.target)?;
                writeln!(f, " Protocol: {:?}", task.meta.protocol)?;
                writeln!(f, " Type: {:?}", task.meta.type_query)?;
                writeln!(f, " Subject:")?;
                for line in task.meta.subject.lines() {
                    writeln!(f, "     {}", line)?;
                }

                let status = match &task.data.result {
                    TaskResult::NoResponseError(_) => "Error: timeout",
                    _ => "Success",
                };

                // ============================================================
                // Статус и данные
                // ============================================================
                writeln!(f, "   Status: {}", status)?;

                match &task.data.result {
                    TaskResult::SnmpGet(response) => {
                        writeln!(f, "   Response:")?;
                        for line in response.to_string().lines() {
                            writeln!(f, "     {}", line)?;
                        }
                    }
                    TaskResult::NoResponseError(errors) => {
                        writeln!(f, "   Errors ({}):", errors.len())?;
                        for (i, err) in errors.iter().enumerate() {
                            writeln!(f, "     {}. {}", i + 1, err)?;
                        }
                    }
                    _ => {}
                }

                // ============================================================
                // Метрики
                // ============================================================
                let m = &task.data.metrics;
                writeln!(f, "  📈 Metrics:")?;
                writeln!(f, "     • Total attempts: {}", m.total_attempts)?;
                writeln!(f, "     • Successful: {} ✅", m.successful)?;
                writeln!(f, "     • Errors: {} ❌", m.errors)?;
                writeln!(f, "     • Current latency: {} ms", m.current_latency_ms)?;

                if m.total_attempts > 0 {
                    writeln!(f, "     • Average latency: {} ms", m.avg_latency_ms)?;
                    if m.min_latency_ms != u64::MAX {
                        writeln!(f, "     • Min latency: {} ms", m.min_latency_ms)?;
                    } else {
                        writeln!(f, "     • Min latency: —")?;
                    }
                    writeln!(f, "     • Max latency: {} ms", m.max_latency_ms)?;
                } else {
                    writeln!(f, "     • Average latency: —")?;
                    writeln!(f, "     • Min latency: —")?;
                    writeln!(f, "     • Max latency: —")?;
                }

                // ============================================================
                // История
                // ============================================================
                if !task.history.history().is_empty() {
                    writeln!(f, "  📜 History ({} entries):", task.history.len())?;
                    for (i, entry) in task.history.history().iter().enumerate().take(5) {
                        writeln!(
                            f,
                            "     {}. {} ({} ms)",
                            i + 1,
                            match entry.result {
                                TaskResult::SnmpGet(_) => "✅ Success",
                                TaskResult::NoResponseError(_) => "❌ No response",
                                _ => "❓ Other",
                            },
                            entry.metrics.current_latency_ms
                        )?;
                    }
                    if task.history.len() > 5 {
                        writeln!(f, "     ... and {} more entries", task.history.len() - 5)?;
                    }
                } else {
                    writeln!(f, "  📜 History: (empty)")?;
                }

                writeln!(f, "  ⏱️  Last update: {}", task.data.last_update.format("%H:%M:%S%.3f"))?;
            }
        }

        // ============================================================
        // Футер
        // ============================================================
        writeln!(f, "\n{}", "═".repeat(80))?;
        writeln!(f, "📊 Summary: {} groups, {} tasks", self.groups.len(), self.total_tasks())?;
        writeln!(f, "{}", "═".repeat(80))?;

        Ok(())
    }
}
