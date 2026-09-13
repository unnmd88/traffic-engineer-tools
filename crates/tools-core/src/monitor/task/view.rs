use chrono::{DateTime, Local};
use tokio::time::Duration;

use crate::{
    monitor::{
        task::{SpecRevision, TaskId, TaskStatus},
        usecase::UseCaseOutput,
    },
    polling::{Metrics, Response},
};

/// Неизменяемое представление одной задачи для интерфейса (read-model).
///
/// Строится из `TaskEntry` (см. `orchestrator`), а не из внутренних типов напрямую.
/// `status` — проекция осей управления, не хранимое поле.
#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub name: String,
    pub revision: SpecRevision,
    pub target: String,
    pub status: TaskStatus,
    pub interval: Duration,
    pub limit: u64,
    pub metrics: Metrics,
    pub result: Option<Response<UseCaseOutput>>,
    pub history: Vec<HistoryEntryView>,
}

/// Одна строка сжатой истории (для отображения).
#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Local>,
    pub result: Option<Response<UseCaseOutput>>,
}

/// Полный снапшот монитора: первичная синхронизация (`get_snapshot`).
#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub tasks: Vec<TaskView>,
}
