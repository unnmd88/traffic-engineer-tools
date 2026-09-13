use thiserror::Error;

/// Ошибка валидации задачи (`TaskSpec`).
#[derive(Error, Debug, Clone)]
pub enum TaskError {
    #[error("task name must not be empty")]
    EmptyName,
}
