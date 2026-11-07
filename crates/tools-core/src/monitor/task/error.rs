use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum TaskError {
    #[error("task name must not be empty")]
    EmptyName,
}
