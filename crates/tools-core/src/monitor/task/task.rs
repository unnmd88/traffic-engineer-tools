use std::collections::VecDeque;

use crate::monitor::{
    probe::{OutcomeStatus, ProbeExecution},
    task::{ExecutionMetrics, TaskConfig, TaskSpec},
};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, Display};

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

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Utc>,
    pub result: ProbeExecution,
}

/// Кольцо последних N результатов. `deep_history == 0` → всегда пустое.
#[derive(Clone, Debug)]
pub struct History {
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

    pub fn deep(&self) -> usize {
        self.deep
    }

    pub fn len(&self) -> usize {
        self.history.len()
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Health {
    pub last_execution_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u64,
    pub last_error: Option<String>,
}

impl Health {
    pub fn record(&mut self, execution: &ProbeExecution) {
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
pub struct Task {
    id: TaskId,
    spec: TaskSpec,
    last_result: Option<ProbeExecution>,
    history: History,
    metrics: ExecutionMetrics,
    health: Health,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(id: TaskId, spec: TaskConfig) -> Self {
        let dt = Utc::now();
        let deep_history = spec.deep_history();
        let spec = TaskSpec::new(spec);
        Self {
            id,
            spec,
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

    pub fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    pub fn last_result(&self) -> Option<&ProbeExecution> {
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

    pub fn apply_execution(&mut self, result: ProbeExecution) {
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

    /// Заменить спеку (value object) и поднять версию.
    pub fn update_spec(&mut self, spec: TaskConfig) {
        self.spec = self.spec.next(spec);
        self.updated_at = Utc::now();
    }

    /// Новый запуск: сброс накопительных метрик и здоровья (result/history остаются).
    pub fn reset_run(&mut self) {
        self.metrics = ExecutionMetrics::default();
        self.health = Health::default();
        self.updated_at = Utc::now();
    }
}
