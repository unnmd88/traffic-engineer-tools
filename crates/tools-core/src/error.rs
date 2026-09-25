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
    SnmpQuery(#[from] crate::monitor::adapter::SnmpQueryError),
    #[error(transparent)]
    Task(#[from] crate::monitor::task::TaskError),
    #[error(transparent)]
    AdapterBuild(#[from] crate::monitor::adapter::AdapterBuildError),
    #[error(transparent)]
    MonitorOrchestrator(#[from] crate::monitor::runtime::error::OrchestratorError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
}
