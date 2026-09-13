use chrono::{DateTime, Local};
use tokio::time::Duration;

use crate::{
    monitor::{
        adapter::AdapterOutput,
        task::{SpecRevision, TaskId, TaskStatus},
    },
    polling::{Metrics, Response},
};

/// Неизменяемое представление одной задачи для интерфейса (read-model).
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
    pub result: Option<Response<AdapterOutput>>,
    pub history: Vec<HistoryEntryView>,
}

#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Local>,
    pub result: Option<Response<AdapterOutput>>,
}

/// Полный снапшот монитора задач.
#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub tasks: Vec<TaskView>,
}
