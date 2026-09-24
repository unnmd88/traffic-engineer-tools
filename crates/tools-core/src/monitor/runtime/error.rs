use thiserror::Error;

use crate::monitor::task::TaskId;

#[derive(Error, Debug, Clone)]
pub enum SupervisorError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("supervisor channel closed")]
    ChannelClosed,
}
