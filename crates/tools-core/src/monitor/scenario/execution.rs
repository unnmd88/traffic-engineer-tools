use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{
    monitor::{OperationContext, OperationReport},
    polling::{PollDetails, PollOutcome},
    snmp::{SnmpError, SnmpGetSample},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PanicStage {
    Build,
    Execution,
    Runtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeStatus {
    Succeeded,
    Degraded,
    Failed,
}

/// Scenario-specific policy, attempts, and results; operation timing is stored separately.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", content = "report", rename_all = "snake_case")]
pub enum ScenarioOutcome {
    /// Evidence and samples from an SNMP GET operation.
    SnmpGet(PollDetails<Vec<SnmpGetSample>, SnmpError>),
    /// Evidence and acknowledgement from an SNMP SET operation.
    SnmpSet(PollDetails<(), SnmpError>),
}

impl ScenarioOutcome {
    pub const fn status(&self) -> OutcomeStatus {
        match self {
            Self::SnmpGet(response) => response_status(response),
            Self::SnmpSet(response) => response_status(response),
        }
    }
}

const fn response_status<T, E>(report: &PollDetails<T, E>) -> OutcomeStatus {
    match &report.outcome {
        PollOutcome::Succeeded(_) => OutcomeStatus::Succeeded,
        PollOutcome::RetryLimitReached | PollOutcome::NonRetryableFailure => OutcomeStatus::Failed,
    }
}

/// A retained normal operation result with its original execution context.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScenarioExecution {
    /// The operation and applied configuration that produced this result.
    pub context: OperationContext,
    /// The complete operation report, shared with operation-finished events.
    pub report: OperationReport,
}

impl ScenarioExecution {
    /// Retains an operation's original context and complete report.
    pub const fn new(context: OperationContext, report: OperationReport) -> Self {
        Self { context, report }
    }

    /// Returns the operation completion time.
    pub const fn finished_at(&self) -> DateTime<Utc> {
        self.report.finished_at
    }

    /// Returns the monotonic operation duration.
    pub const fn elapsed(&self) -> Duration {
        self.report.elapsed
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[must_use]
pub struct Generation(u64);

impl Generation {
    pub const fn first() -> Self {
        Self(0)
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
