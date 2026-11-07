use serde::{Deserialize, Serialize};

use thiserror::Error;

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ScenarioExecutionError {
    #[error(transparent)]
    Polling(#[from] crate::polling::FatalError),
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ScenarioBuildError {
    #[error("build timeout")]
    Timeout,
    #[error("can't create snmp-driver, try again later")]
    SnmpClientCreate,
    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScenarioSpecError {
    #[error("poll schedule duration must be greater than zero")]
    ScheduleDurationMustBePositive,
}
