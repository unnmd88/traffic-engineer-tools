use std::{collections::VecDeque, mem};

use crate::{
    monitor::{
        adapter::AdapterOutput,
        task::{SpecRevision, TaskConfig, TaskSpec},
    },
    polling::{Metrics, PollConfig, Response},
};
use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq)]
pub enum TaskStatus {
    Idle,
    Starting,
    Active,
    Stopped,
    Completed,
    Restarting,
    Failed,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);

/// Одна запись истории: предыдущий результат опроса.
#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Local>,
    pub result: Response<AdapterOutput>,
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

/// Здоровье: агрегат последних опросов (для диагностики «почему не шёл»).
#[derive(Clone, Debug, Default)]
pub struct Health {
    pub last_poll_at: Option<DateTime<Local>>,
    pub consecutive_no_response: u32,
    pub last_error: Option<String>,
}

impl Health {
    pub fn record(&mut self, r: &Response<AdapterOutput>) {
        self.last_poll_at = Some(Local::now());
        match r {
            Response::Success { .. } => {
                self.consecutive_no_response = 0;
                self.last_error = None;
            }
            Response::NoResponse { errors, .. } => {
                self.consecutive_no_response += 1;
                self.last_error = errors.last().map(|e| e.message.clone());
            }
        }
    }
}

/// Данные одной задачи: что опрашивать + что получили. Без статуса (он производный).
#[derive(Clone, Debug)]
pub struct Task {
    id: TaskId,
    spec: TaskSpec,
    last_result: Option<Response<AdapterOutput>>,
    history: History,
    metrics: Metrics,
    health: Health,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl Task {
    pub fn new(id: TaskId, spec: TaskConfig) -> Self {
        let dt = Local::now();
        let deep_history = spec.deep_history();
        let spec = TaskSpec::new(spec);
        Self {
            id,
            spec,
            last_result: None,
            metrics: Metrics::default(),
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

    pub fn last_result(&self) -> Option<&Response<AdapterOutput>> {
        self.last_result.as_ref()
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics
    }

    pub fn created_at(&self) -> &DateTime<Local> {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTime<Local> {
        &self.updated_at
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn health(&self) -> &Health {
        &self.health
    }

    /// Пришёл результат опроса: старый → история, новый → текущий, обновить метрики и здоровье.
    pub fn apply_poll(&mut self, result: Response<AdapterOutput>, metrics: Metrics) {
        self.health.record(&result);
        let ts = Local::now();
        if let Some(prev) = mem::replace(&mut self.last_result, Some(result)) {
            self.history.push(HistoryEntry {
                timestamp: ts,
                result: prev,
            });
        }
        self.metrics = metrics;
        self.updated_at = ts;
    }

    /// Заменить спеку (value object) и поднять версию.
    pub fn update_spec(&mut self, spec: TaskConfig) {
        self.spec = self.spec.next(spec);
        self.updated_at = Local::now();
    }

    /// Новый запуск: сброс накопительных метрик и здоровья (result/history остаются).
    pub fn reset_run(&mut self) {
        self.metrics = Metrics::default();
        self.health = Health::default();
        self.updated_at = Local::now();
    }
}
