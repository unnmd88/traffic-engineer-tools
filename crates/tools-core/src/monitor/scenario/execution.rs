use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{
    monitor::OperationContext,
    polling::{PollOutcome, PollReport},
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ScenarioOutcome {
    SnmpGet(PollReport<Vec<SnmpGetSample>, SnmpError>),
    SnmpSet(PollReport<(), SnmpError>),
}

impl ScenarioOutcome {
    pub const fn status(&self) -> OutcomeStatus {
        match self {
            Self::SnmpGet(response) => response_status(response),
            Self::SnmpSet(response) => response_status(response),
        }
    }
}

const fn response_status<T, E>(report: &PollReport<T, E>) -> OutcomeStatus {
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
    /// The complete protocol-specific report, including timing and attempts.
    pub outcome: ScenarioOutcome,
}

impl ScenarioExecution {
    /// Retains an operation's original context with its complete result.
    pub const fn new(context: OperationContext, outcome: ScenarioOutcome) -> Self {
        Self { context, outcome }
    }

    /// Returns the completion time from the enclosed operation report.
    pub const fn finished_at(&self) -> DateTime<Utc> {
        match &self.outcome {
            ScenarioOutcome::SnmpGet(report) => report.finished_at,
            ScenarioOutcome::SnmpSet(report) => report.finished_at,
        }
    }

    /// Returns the monotonic duration from the enclosed operation report.
    pub const fn elapsed(&self) -> Duration {
        match &self.outcome {
            ScenarioOutcome::SnmpGet(report) => report.elapsed,
            ScenarioOutcome::SnmpSet(report) => report.elapsed,
        }
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
