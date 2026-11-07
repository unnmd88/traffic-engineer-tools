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
    SnmpQuery(#[from] crate::snmp::SnmpQueryError),
    #[error(transparent)]
    Task(#[from] crate::monitor::TaskError),
    #[error(transparent)]
    ScenarioBuild(#[from] crate::monitor::ScenarioBuildError),
    #[error(transparent)]
    ScenarioExecution(#[from] crate::monitor::ScenarioExecutionError),
    #[error(transparent)]
    MonitorCommand(#[from] crate::monitor::MonitorCommandError),
    #[error(transparent)]
    MonitorApplication(#[from] crate::monitor::ApplicationError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
    #[error(transparent)]
    ScenarioSpec(#[from] crate::monitor::ScenarioSpecError),
}
