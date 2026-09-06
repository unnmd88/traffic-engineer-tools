use thiserror::Error;

use crate::snmp::SnmpError;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("ASCII conversion error: {0}")]
    Ascii(#[from] AsciiError),
    #[error("SNMP error: {0}")]
    Snmp(#[from] SnmpError),
    //#[error("IO error: {0}")]
    //Io(#[from] std::io::Error),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("No response: {0}")]
    NoResponse(String),
    #[error("ParseError error: {0}")]
    Parse(#[from] ParseError),
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Build `Monitor` error: {0}")]
    BuildMonitorError(#[from] BuildMonitorError),
    #[error("Update error: {0}")]
    Update(#[from] UpdateError),
    #[error("{0}")]
    Adapter(#[from] AdapterError),
    #[error("{0}")]
    TaskRepository(#[from] TaskRepositoryError),
    #[error("{0}")]
    Application(#[from] ApplicationError),
    #[error("{0}")]
    Orchestrator(#[from] OrchestratorError),
    #[error("{0}")]
    PollingConfig(#[from] crate::polling::ConfigError),
    #[error("{0}")]
    Task(#[from] crate::monitor::task::TaskError),
}

#[derive(Error, Debug, Clone)]
pub enum OrchestratorError {
    #[error("build use-case failed: {0}")]
    Build(#[from] BuildMonitorError),
    #[error(transparent)]
    TaskRepository(#[from] TaskRepositoryError),
    #[error("task not found: {task_id}")]
    TaskNotFound { task_id: String },
    #[error("orchestrator channel closed")]
    ChannelClosed,
}

#[derive(Error, Debug, Clone)]
pub enum UpdateError {
    #[error("Can`t create ada adapter {message}")]
    Adapter { message: String },
}

#[derive(Error, Debug, Clone)]
pub enum ApplicationError {
    #[error("Cat`n subscribe to for task repository updates: {reason}")]
    RepositorySubscribe { reason: String },
    #[error("Cat`n get snapshot of task repository: {reason}")]
    GetSnapshot { reason: String },
}

#[derive(Error, Debug, Clone)]
pub enum TaskRepositoryError {
    #[error("Task with id={task_id} not fond in repository.")]
    TaskNotFound { task_id: String },
}

#[derive(Error, Debug, Clone)]
pub enum AdapterError {
    #[error("{message}")]
    Create { message: String },
}

#[derive(Error, Debug, Clone)]
pub enum BuildMonitorError {
    #[error("Can't create snmp-driver. Try again later.")]
    SnmpClientCreate,
    #[error("Invalid ip-address: {ip}")]
    InvalidIpAddress { ip: String },
    #[error("{message}")]
    InvalidSnmpProfile { message: String },
    #[error("{message}")]
    SnmpProfileMustBeProvided { message: String },
    #[error("OID at position {pos}: unknown alias '{alias}'")]
    UnknownAlias { pos: usize, alias: String },
    #[error("Community string can't be empty")]
    SnmpCommunityIsEmpty,
    #[error("Invalid length for community string (min: {min}, max: {max}, got: {provide})")]
    SnmpCommunityInvalidLength {
        min: usize,
        max: usize,
        provide: usize,
    },
    #[error("Invalid snmp-oid(pos: {pos}): {oid}")]
    InvalidSnmpOid { pos: usize, oid: String },
    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug, Clone)]
pub enum ParseError {
    #[error("Invalid length: {message} (min: {min}, max: {max}, got: {provide})")]
    InvalidLength {
        message: String,
        min: usize,
        max: usize,
        provide: usize,
    },
    #[error("Expected {expected}, but got {actual}")]
    InvalidType { expected: String, actual: String },
    #[error("{name} can`t be empty")]
    CantBeEmpty { name: String },
    #[error("{message}")]
    Common { message: String },
}

#[derive(Debug, Clone, Error)]
pub enum AsciiError {
    #[error("String is empty")]
    Empty,

    #[error("Contains non-ASCII characters: {0:?}")]
    NonAsciiCharacters(Vec<char>),

    #[error("Invalid code: {0}")]
    InvalidCode(String),

    #[error("Invalid prefix: {0}")]
    InvalidPrefix(String),

    #[error("Invalid length: {0}")]
    InvalidLength(String),

    #[error("Length mismatch: expected {expected}, got {actual}")]
    LengthMismatch { expected: usize, actual: usize },

    #[error("Invalid format")]
    InvalidFormat,
}
