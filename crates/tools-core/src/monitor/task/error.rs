use thiserror::Error;

/// Ошибка валидации задачи (`TaskSpec`).
#[derive(Error, Debug, Clone)]
pub enum TaskError {
    #[error("task name must not be empty")]
    EmptyName,
}

/// Ошибка хранилища задач.
#[derive(Error, Debug, Clone)]
pub enum TaskRepositoryError {
    #[error("task with id={task_id} not found in repository")]
    TaskNotFound { task_id: String },
}

/// Ошибка валидации запроса (`QuerySnmpGet::from_raw`).
#[derive(Error, Debug, Clone)]
pub enum QueryError {
    #[error("invalid ip-address: {ip}")]
    InvalidIpAddress { ip: String },
    #[error("{message}")]
    InvalidSnmpProfile { message: String },
    #[error("{message}")]
    SnmpProfileMustBeProvided { message: String },
    #[error("oid at position {pos}: unknown alias '{alias}'")]
    UnknownAlias { pos: usize, alias: String },
    #[error("community string can't be empty")]
    SnmpCommunityIsEmpty,
    #[error("invalid length for community string (min: {min}, max: {max}, got: {provide})")]
    SnmpCommunityInvalidLength { min: usize, max: usize, provide: usize },
    #[error("invalid snmp-oid(pos: {pos}): {oid}")]
    InvalidSnmpOid { pos: usize, oid: String },
    #[error("{0}")]
    Other(String),
}
