use std::sync::Arc;

use thiserror::Error;

use crate::monitor::task::TaskId;

#[derive(Error, Debug, Clone)]
pub enum ApplicationError {
    #[error(transparent)]
    Orchestrator(#[from] OrchestratorError),
    #[error("application runtime failed: {0}")]
    Runtime(#[source] Arc<tokio::task::JoinError>),
}

#[derive(Error, Debug, Clone)]
pub enum OrchestratorError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("orchestrator channel closed")]
    ChannelClosed,
}
