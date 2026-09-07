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
    TaskRepository(#[from] crate::monitor::task::TaskRepositoryError),
    #[error(transparent)]
    UseCaseBuild(#[from] crate::monitor::usecase::UseCaseBuildError),
    #[error(transparent)]
    Orchestrator(#[from] crate::monitor::orchestrator::OrchestratorError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
}
