use thiserror::Error;

use crate::monitor::{
    task::TaskRepositoryError,
    usecase::UseCaseBuildError,
};

#[derive(Error, Debug, Clone)]
pub enum OrchestratorError {
    #[error("build use-case failed: {0}")]
    Build(#[from] UseCaseBuildError),
    #[error(transparent)]
    TaskRepository(#[from] TaskRepositoryError),
    #[error("task not found: {task_id}")]
    TaskNotFound { task_id: String },
    #[error("orchestrator channel closed")]
    ChannelClosed,
}
