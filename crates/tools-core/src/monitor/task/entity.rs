use std::{collections::VecDeque, mem};

use crate::{
    monitor::{
        task::{SpecRevision, TaskId, TaskSpec, TaskSpecPayload, UseCaseQuery},
        usecase::UseCaseOutput,
    },
    polling::{Metrics, PollConfig, Response},
};
use chrono::{DateTime, Local};

/// Одна запись истории: предыдущий результат опроса.
#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Local>,
    pub result: Response<UseCaseOutput>,
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
    pub fn record(&mut self, r: &Response<UseCaseOutput>) {
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
pub struct TaskEntity {
    id: TaskId,
    spec: TaskSpec,
    result: Option<Response<UseCaseOutput>>,
    history: History,
    metrics: Metrics,
    health: Health,
    created_at: DateTime<Local>,
    updated_at: DateTime<Local>,
}

impl TaskEntity {
    pub fn new(id: TaskId, payload: TaskSpecPayload) -> Self {
        let dt = Local::now();
        let deep_history = payload.deep_history();
        let spec = TaskSpec::new(payload);
        Self {
            id,
            spec,
            result: None,
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

    pub fn spec_payload(&self) -> &TaskSpecPayload {
        self.spec.payload()
    }

    pub fn spec_revision(&self) -> SpecRevision {
        self.spec.revision()
    }

    pub fn name(&self) -> &str {
        self.spec.payload().name()
    }

    pub fn query(&self) -> &UseCaseQuery {
        self.spec.payload().query()
    }

    pub fn poll_config(&self) -> PollConfig {
        *self.spec.payload().poll_config()
    }

    pub fn last_result(&self) -> Option<&Response<UseCaseOutput>> {
        self.result.as_ref()
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

    /// Пришёл результат опроса: старый → история, новый → текущий, обновить метрики и здоровье.
    pub fn apply_poll(&mut self, result: Response<UseCaseOutput>, metrics: Metrics) {
        self.health.record(&result);
        let ts = Local::now();
        if let Some(prev) = mem::replace(&mut self.result, Some(result)) {
            self.history.push(HistoryEntry {
                timestamp: ts,
                result: prev,
            });
        }
        self.metrics = metrics;
        self.updated_at = ts;
    }

    /// Заменить спеку (value object) и поднять версию.
    pub fn update_spec(&mut self, spec: TaskSpecPayload) {
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
