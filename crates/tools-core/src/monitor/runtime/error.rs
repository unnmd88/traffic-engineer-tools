use std::sync::Arc;

use thiserror::Error;

use crate::monitor::task::TaskId;

#[derive(Error, Debug, Clone)]
pub enum ApplicationError {
    #[error(transparent)]
    Command(#[from] MonitorCommandError),
    #[error("application runtime failed: {0}")]
    Runtime(#[source] Arc<tokio::task::JoinError>),
}

#[derive(Error, Debug, Clone)]
pub enum MonitorCommandError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    /// A new limit requires a new execution after the current one ends.
    #[error("task {0} is already running with a different limit")]
    TaskAlreadyRunning(TaskId),
    #[error("monitor command channel closed")]
    ChannelClosed,
}
