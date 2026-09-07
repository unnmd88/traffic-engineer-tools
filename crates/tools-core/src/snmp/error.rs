use std::net::SocketAddr;

use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum SnmpError {
    #[error("network error communicating with {target}: {reason}")]
    Network { target: SocketAddr, reason: String },
    #[error("SNMP timeout for {target} after {retries} retries")]
    Timeout { target: SocketAddr, retries: u32 },
    #[error("authentication failed for {target}")]
    Auth { target: SocketAddr },
    #[error("SNMP protocol error from {target}: {status} at index {index}")]
    Protocol {
        target: SocketAddr,
        status: String,
        index: u32,
        oid: Option<String>,
    },
    #[error("Invalid OID: {0}")]
    InvalidOid(String),
    #[error("Error parse raw SNMP value: {0}")]
    ParseRawValue(String),
    #[error("Unexpected value in oid: expected: {expected} actual: {actual}")]
    UnexpectedValueType { expected: String, actual: String },
    #[error("Error to set scn. Profile: {profile}, Reason: {message}")]
    ScnError { profile: String, message: String },
    #[error("Can`t resolve oid: {0}")]
    ResolveOid(String),
    #[error("Internal SNMP error: {0}")]
    Internal(String),
    #[error("Convert bytes to scn error: {0}")]
    ConvertScn(String),
    #[error("Unsupported value for snmp-set: {value}")]
    UnsupportedForSet { value: String },
}

/// Ошибка парсинга значения в доменный тип.
#[derive(Error, Debug, Clone)]
pub enum ParseError {
    #[error("invalid length: {message} (min: {min}, max: {max}, got: {provide})")]
    InvalidLength {
        message: String,
        min: usize,
        max: usize,
        provide: usize,
    },
    #[error("expected {expected}, but got {actual}")]
    InvalidType { expected: String, actual: String },
    #[error("{name} can`t be empty")]
    CantBeEmpty { name: String },
    #[error("{message}")]
    Common { message: String },
}
