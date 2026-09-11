use thiserror::Error;

use crate::monitor::{task::TaskId, usecase::UseCaseBuildError};

#[derive(Error, Debug, Clone)]
pub enum OrchestratorError {
    #[error("build use-case failed: {0}")]
    Build(#[from] UseCaseBuildError),
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("orchestrator channel closed")]
    ChannelClosed,
}
