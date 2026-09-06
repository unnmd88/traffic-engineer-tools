use thiserror::Error;

/// Ошибка валидации задачи (`TaskSpec`).
#[derive(Error, Debug, Clone)]
pub enum TaskError {
    #[error("task name must not be empty")]
    EmptyName,
}

/// Ошибка хранилища задач.
#[derive(Error, Debug, Clone)]
pub enum TaskRepositoryError {
    #[error("task with id={task_id} not found in repository")]
    TaskNotFound { task_id: String },
}
