use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::monitor::{PanicStage, ScenarioBuildError, ScenarioExecutionError};

use super::TaskStatus;

/// A user command whose application caused a task state transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TaskCommandKind {
    /// Start a task.
    Start,
    /// Pause execution after the scenario acknowledges the request.
    Pause,
    /// Resume execution after the scenario acknowledges the request.
    Resume,
    /// Stop a task.
    Stop,
}

/// A typed scenario failure used to explain task recovery independently of operation results.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ScenarioFailure {
    /// Preparation could not complete.
    Preparation(ScenarioBuildError),
    /// Execution could not continue.
    Execution(ScenarioExecutionError),
    /// A panic was caught in scenario code.
    #[non_exhaustive]
    Panicked {
        /// Where the panic was caught.
        stage: PanicStage,
        /// The panic payload rendered as text.
        reason: String,
    },
}

impl std::fmt::Display for ScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Preparation(error) => error.fmt(f),
            Self::Execution(error) => error.fmt(f),
            Self::Panicked { stage, reason } => {
                let label = match stage {
                    PanicStage::Build => "build",
                    PanicStage::Execution => "execution",
                    PanicStage::Runtime => "scenario",
                };
                write!(f, "{label}: {reason}")
            }
        }
    }
}

/// Why a task actually changed state; accepting a command alone is not a transition.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TaskTransitionCause {
    /// A user command took effect.
    Command(TaskCommandKind),
    /// The initial task was started automatically by the monitor.
    InitialStart,
    /// Preparation completed and execution became ready.
    ScenarioReady,
    /// Applying a replacement configuration changed execution state.
    ConfigurationChanged,
    /// The scenario finished according to its own completion condition.
    ScenarioCompleted,
    /// The task reached its configured operation limit.
    #[non_exhaustive]
    LimitReached {
        /// The configured limit that triggered completion.
        limit: u64,
    },
    /// A scenario failure caused recovery or termination.
    Failure(Box<ScenarioFailure>),
    /// A scheduled restart became due.
    #[non_exhaustive]
    RestartDue {
        /// The restart-attempt number from the consumed plan.
        attempt: u32,
    },
    /// The monitor was shut down.
    Shutdown,
}

/// The scheduled next scenario start; the timestamp does not guarantee actual readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RestartPlan {
    /// The restart-attempt number, separate from attempts within an operation.
    pub attempt: u32,
    /// UTC time at which the next start is scheduled to become eligible.
    pub scheduled_at: DateTime<Utc>,
}

/// A task state change and the context needed to explain it to an observer.
///
/// The same transition is published as an event and retained in the task snapshot.
/// These passive fields do not validate legal state transitions on deserialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TaskTransition {
    /// UTC time at which the state was changed, before event publication.
    pub at: DateTime<Utc>,
    /// State immediately before the transition.
    pub previous: TaskStatus,
    /// State immediately after the transition.
    pub current: TaskStatus,
    /// The typed reason that caused the transition.
    pub cause: TaskTransitionCause,
    /// The next start plan when entering the restart-waiting state.
    pub restart: Option<RestartPlan>,
}
