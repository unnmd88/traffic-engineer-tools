use thiserror::Error;

/// Зонт-агрегатор ошибок ядра: собирает модульные ошибки через `#[from]`.
#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error(transparent)]
    Snmp(#[from] crate::snmp::SnmpError),
    #[error(transparent)]
    Parse(#[from] crate::snmp::ParseError),
    #[error(transparent)]
    Ascii(#[from] crate::ascii::AsciiError),
    #[error(transparent)]
    Query(#[from] crate::monitor::task::QueryError),
    #[error(transparent)]
    Task(#[from] crate::monitor::task::TaskError),
    #[error(transparent)]
    UseCaseBuild(#[from] crate::monitor::usecase::UseCaseBuildError),
    #[error(transparent)]
    MonitorSupervisor(#[from] crate::monitor::runtime::error::SupervisorError),
    #[error(transparent)]
    MonitorProjector(#[from] crate::monitor::runtime::error::ProjectorError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
}
