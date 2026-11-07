use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use derive_more::{Constructor, Display};

use crate::monitor::{
    scenario::{OutcomeStatus, ScenarioExecution},
    task::{ExecutionMetrics, TaskConfig},
};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub enum TaskStatus {
    Idle,
    Starting,
    Active,
    Paused,
    Stopped,
    Completed,
    Restarting,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor)]
pub struct SpecRevision(pub u64);

impl SpecRevision {
    pub fn next(self) -> Self {
        Self::new(self.0 + 1)
    }
}

#[derive(Clone, Debug)]
pub(in crate::monitor) struct HistoryEntry {
    pub timestamp: DateTime<Utc>,
    pub result: ScenarioExecution,
}

/// Кольцо последних N результатов. `deep_history == 0` → всегда пустое.
#[derive(Clone, Debug)]
pub(in crate::monitor) struct History {
    deep: usize,
    history: VecDeque<HistoryEntry>,
}

impl History {
    pub fn new(deep_history: u8) -> Self {
        let max = deep_history as usize;
        Self {
            deep: max,
            history: VecDeque::with_capacity(max),
        }
    }

    pub fn push(&mut self, entry: HistoryEntry) {
        if self.deep == 0 {
            return;
        }
        if self.history.len() >= self.deep {
            self.history.pop_back();
        }
        self.history.push_front(entry);
    }

    pub fn iter(&self) -> impl Iterator<Item = &HistoryEntry> {
        self.history.iter()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Health {
    pub last_execution_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u64,
    pub last_error: Option<String>,
}

impl Health {
    pub fn record(&mut self, execution: &ScenarioExecution) {
        self.last_execution_at = Some(execution.finished_at);
        match execution.outcome.status() {
            OutcomeStatus::Failed => self.consecutive_failures += 1,
            OutcomeStatus::Succeeded | OutcomeStatus::Degraded => self.consecutive_failures = 0,
        }
        self.last_error = None;
    }

    pub fn record_error(&mut self, reason: String) {
        self.last_error = Some(reason);
    }

    pub fn clear_error(&mut self) {
        self.last_error = None;
    }
}

#[derive(Clone, Debug)]
pub(in crate::monitor) struct Task {
    id: TaskId,
    config: TaskConfig,
    revision: SpecRevision,
    config_updated_at: DateTime<Utc>,
    last_result: Option<ScenarioExecution>,
    history: History,
    metrics: ExecutionMetrics,
    health: Health,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(id: TaskId, config: TaskConfig) -> Self {
        let dt = Utc::now();
        let deep_history = config.deep_history();
        Self {
            id,
            config,
            revision: SpecRevision::new(1),
            config_updated_at: dt,
            last_result: None,
            metrics: ExecutionMetrics::default(),
            health: Health::default(),
            history: History::new(deep_history),
            created_at: dt,
            updated_at: dt,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub const fn config(&self) -> &TaskConfig {
        &self.config
    }

    pub const fn revision(&self) -> SpecRevision {
        self.revision
    }

    pub const fn config_updated_at(&self) -> &DateTime<Utc> {
        &self.config_updated_at
    }

    pub fn last_result(&self) -> Option<&ScenarioExecution> {
        self.last_result.as_ref()
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics
    }

    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn health(&self) -> &Health {
        &self.health
    }

    pub fn apply_execution(&mut self, result: ScenarioExecution) {
        self.health.record(&result);
        self.metrics.record(result.outcome.status(), result.elapsed);
        self.updated_at = result.finished_at;
        if let Some(prev) = self.last_result.replace(result) {
            self.history.push(HistoryEntry {
                timestamp: prev.finished_at,
                result: prev,
            });
        }
    }

    /// Записать причину сбоя (для диагностики «почему Restarting»).
    pub fn record_error(&mut self, reason: String) {
        self.health.record_error(reason);
    }

    /// Сбросить причину после успешной сборки/опроса.
    pub fn clear_error(&mut self) {
        self.health.clear_error();
    }

    pub fn update_config(&mut self, config: TaskConfig) {
        let revision = self.revision.next();
        let now = Utc::now();
        self.config = config;
        self.revision = revision;
        self.config_updated_at = now;
        self.updated_at = now;
    }

    /// Новый запуск: сброс накопительных метрик и здоровья (result/history остаются).
    pub fn reset_run(&mut self) {
        self.metrics = ExecutionMetrics::default();
        self.health = Health::default();
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::scenario::{PollSchedule, ScenarioOutcome, ScenarioSpec, SnmpGetScenarioSpec},
        polling::{AttemptConfig, Response},
        snmp::SnmpGetSpec,
    };
    use tokio::time::Duration;

    fn config(name: &str) -> TaskConfig {
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 2, Duration::ZERO).unwrap();
        let scenario = ScenarioSpec::SnmpGet(
            SnmpGetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(1),
                },
                attempt,
            )
            .unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, 10, 3).unwrap()
    }

    fn failed_execution(finished_at: DateTime<Utc>) -> ScenarioExecution {
        ScenarioExecution {
            finished_at,
            elapsed: Duration::from_millis(300),
            outcome: ScenarioOutcome::SnmpGet(Response::NoResponse {
                timestamp: finished_at,
                attempts: 3,
                errors: vec![],
                elapsed: Duration::from_millis(300),
            }),
        }
    }

    #[test]
    fn execution_and_restart_preserve_configuration_metadata() {
        let mut task = Task::new(TaskId(1), config("first"));
        let config_updated_at = *task.config_updated_at();
        let finished_at = config_updated_at + chrono::Duration::seconds(1);

        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.config_updated_at(), task.created_at());
        task.apply_execution(failed_execution(finished_at));

        assert_eq!(task.updated_at(), &finished_at);
        assert_eq!(task.config_updated_at(), &config_updated_at);
        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.metrics().executions, 1);
        assert_eq!(task.metrics().failed, 1);

        task.reset_run();
        assert_eq!(task.config_updated_at(), &config_updated_at);
        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.metrics(), ExecutionMetrics::default());
        assert_eq!(task.last_result().unwrap().finished_at, finished_at);
    }

    #[test]
    fn config_update_advances_revision_and_preserves_execution_state() {
        let mut task = Task::new(TaskId(1), config("first"));
        let created_at = *task.created_at();
        let finished_at = created_at + chrono::Duration::seconds(1);
        task.apply_execution(failed_execution(finished_at));
        let metrics = task.metrics();
        let before_update = Utc::now();

        task.update_config(config("updated"));

        assert_eq!(task.config().name(), "updated");
        assert_eq!(task.revision(), SpecRevision::new(2));
        assert!(*task.config_updated_at() >= before_update);
        assert!(*task.config_updated_at() <= Utc::now());
        assert_eq!(task.config_updated_at(), task.updated_at());
        assert_eq!(task.created_at(), &created_at);
        assert_eq!(task.metrics(), metrics);
        assert_eq!(task.last_result().unwrap().finished_at, finished_at);

        task.update_config(config("third"));
        assert_eq!(task.config().name(), "third");
        assert_eq!(task.revision(), SpecRevision::new(3));
    }
}
