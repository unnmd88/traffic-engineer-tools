use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{polling::Response, snmp::SnmpGetSample};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeStatus {
    Succeeded,
    Degraded,
    Failed,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ProbeOutcome {
    SnmpGet(Response<Vec<SnmpGetSample>>),
    SnmpSet(Response<()>),
}

impl ProbeOutcome {
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
pub struct ProbeExecution {
    pub finished_at: DateTime<Utc>,
    pub elapsed: Duration,
    pub outcome: ProbeOutcome,
}
