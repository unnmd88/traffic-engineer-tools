use thiserror::Error;

/// Зонт-агрегатор ошибок ядра: собирает модульные ошибки через `#[from]`.
#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error(transparent)]
    Snmp(#[from] crate::snmp::SnmpError),
    #[error(transparent)]
    Parse(#[from] crate::snmp::ParseError),
    #[error(transparent)]
    Scn(#[from] crate::scn::ScnError),
    #[error(transparent)]
    SnmpQuery(#[from] crate::monitor::probe::SnmpQueryError),
    #[error(transparent)]
    Task(#[from] crate::monitor::task::TaskError),
    #[error(transparent)]
    ProbeBuild(#[from] crate::monitor::probe::ProbeBuildError),
    #[error(transparent)]
    Probe(#[from] crate::monitor::probe::ProbeError),
    #[error(transparent)]
    MonitorOrchestrator(#[from] crate::monitor::runtime::error::OrchestratorError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
    #[error(transparent)]
    ProbeSpec(#[from] crate::monitor::probe::ProbeSpecError),
}
