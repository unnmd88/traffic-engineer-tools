use thiserror::Error;

/// Ошибка сборки адаптера в `UseCase::build`.
#[derive(Error, Debug, Clone)]
pub enum UseCaseBuildError {
    #[error("can't create snmp-driver, try again later")]
    SnmpClientCreate,
    #[error("{0}")]
    Other(String),
}
