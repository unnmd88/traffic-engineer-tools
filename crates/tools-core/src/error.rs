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
    ScenarioBuild(#[from] crate::monitor::scenario::ScenarioBuildError),
    #[error(transparent)]
    ScenarioExecution(#[from] crate::monitor::scenario::ScenarioExecutionError),
    #[error(transparent)]
    MonitorOrchestrator(#[from] crate::monitor::runtime::error::OrchestratorError),
    #[error(transparent)]
    MonitorApplication(#[from] crate::monitor::runtime::error::ApplicationError),
    #[error(transparent)]
    PollingConfig(#[from] crate::polling::ConfigError),
    #[error(transparent)]
    ScenarioSpec(#[from] crate::monitor::scenario::ScenarioSpecError),
}
