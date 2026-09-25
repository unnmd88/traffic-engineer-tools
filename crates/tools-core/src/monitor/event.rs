use chrono::{DateTime, Local};

use crate::monitor::task::{TaskId, TaskView};

/// Причина изменения задачи — для live/файла (сток фильтрует по ней).
#[derive(Clone, Debug)]
pub enum ChangeKind {
    Added,
    SpecUpdated,
    Starting,
    Started,
    Restarted,
    Paused,
    Resumed,
    Stopped,
    Completed,
    Failed { reason: String },
    BuildFailed { reason: String },
    Panicked { reason: String },
    Polled,
}

/// Единое доменное событие наружу. Уходит в оба канала: надёжный (запись)
/// и ненадёжный (live). Строит его оркестратор — единственный писатель.
#[derive(Clone, Debug)]
pub enum MonitorEvent {
    TaskChanged {
        task_id: TaskId,
        kind: ChangeKind,
        view: TaskView,
        at: DateTime<Local>,
    },
    TaskRemoved {
        task_id: TaskId,
        view: TaskView,
        at: DateTime<Local>,
    },
}
