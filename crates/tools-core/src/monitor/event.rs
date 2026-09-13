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
    Stopped,
    Completed,
    Failed { reason: String },
    BuildFailed { reason: String },
    Polled,
}

/// Единое событие наружу (тот же тип для broadcast и mpsc).
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

/// Факт: что актор сообщает проектору о задаче.
///
/// Актор — единственный владелец состояния задачи; проектор держит только
/// проекцию (`TaskView`) и раздаёт события наружу.
#[derive(Clone, Debug)]
pub enum TaskFact {
    /// Задача изменилась (добавлена / сменила статус / опросила и т.п.).
    Changed {
        task_id: TaskId,
        kind: ChangeKind,
        view: TaskView,
    },
    /// Задачу удалили — проектор убирает view и шлёт `TaskRemoved`.
    Removed { task_id: TaskId },
    /// Актор упал целиком (баг в собственной логике); данные уже в проекторе.
    Crashed { task_id: TaskId, reason: String },
}
