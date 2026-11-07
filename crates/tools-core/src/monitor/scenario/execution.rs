use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{polling::Response, snmp::SnmpGetSample};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PanicStage {
    Build,
    Execution,
    Runtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeStatus {
    Succeeded,
    Degraded,
    Failed,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ScenarioOutcome {
    SnmpGet(Response<Vec<SnmpGetSample>>),
    SnmpSet(Response<()>),
}

impl ScenarioOutcome {
    pub const fn status(&self) -> OutcomeStatus {
        match self {
            Self::SnmpGet(response) => response_status(response),
            Self::SnmpSet(response) => response_status(response),
        }
    }
}

const fn response_status<T>(response: &Response<T>) -> OutcomeStatus {
    match response {
        Response::Success { .. } => OutcomeStatus::Succeeded,
        Response::NoResponse { .. } => OutcomeStatus::Failed,
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ScenarioExecution {
    pub finished_at: DateTime<Utc>,
    pub elapsed: Duration,
    pub outcome: ScenarioOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
