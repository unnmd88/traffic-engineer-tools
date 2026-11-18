use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScenarioExecution {
    pub finished_at: DateTime<Utc>,
    pub elapsed: Duration,
    pub outcome: ScenarioOutcome,
}

impl ScenarioExecution {
    /// Creates an execution whose timing is taken from its operation report.
    pub fn new(outcome: ScenarioOutcome) -> Self {
        let (finished_at, elapsed) = match &outcome {
            ScenarioOutcome::SnmpGet(report) => (report.finished_at, report.elapsed),
            ScenarioOutcome::SnmpSet(report) => (report.finished_at, report.elapsed),
        };
        Self {
            finished_at,
            elapsed,
            outcome,
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
