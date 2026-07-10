use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("ASCII conversion error: {0}")]
    Ascii(#[from] AsciiError),
    #[error("SNMP error: {0}")]
    Snmp(#[from] SnmpError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum SnmpError {
    #[error("Timeout while connecting to host {0}")]
    TimeOut(String),
    #[error("Host {0} is unreachable")]
    HostUnreachable(String),
    #[error("Invalid OID: {0}")]
    InvalidOid(String),
    #[error("Unknown SNMP error: {0}")]
    Other(String),
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
