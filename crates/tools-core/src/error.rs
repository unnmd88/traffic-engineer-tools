use std::{
    fmt::{self, Display, Formatter},
    net::{IpAddr, SocketAddr},
};

use async_snmp::StorageType;
use thiserror::Error;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct PollErrorContext {
    pub attempt: u8,
    pub elapsed: Duration,
    pub message: String,
}

impl Display for PollErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "attempt: {} elapsed_ms: {} message: {}",
            self.attempt,
            self.elapsed.as_millis(),
            &self.message
        )?;
        Ok(())
    }
}

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("ASCII conversion error: {0}")]
    Ascii(#[from] AsciiError),
    #[error("SNMP error: {0}")]
    Snmp(#[from] SnmpError),
    #[error("Poll error: {0}")]
    Poll(#[from] PollError),
    //#[error("IO error: {0}")]
    //Io(#[from] std::io::Error),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("No response: {0}")]
    NoResponse(String),
    #[error("ParseError error. Reason: {0}")]
    Parse(#[from] ParseError),
    #[error("SnapShot error: {0}")]
    SnapShot(#[from] SnapShotError),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Create `Monitor` error: {0}")]
    CreateMonitorError(#[from] CreateMonitorError),
}

#[derive(Error, Debug, Clone)]
pub enum CreateMonitorError {
    #[error("Invalid ip-address: {ip}. Task position: {task_idx}")]
    InvalidIpAddress { task_idx: usize, ip: String },
    #[error("{message}")]
    InvalidSnmpProfile { message: String },
    #[error("Community string can`t be empty. Task position: {task_idx}")]
    SnmpCommunityIsEmpty { task_idx: usize },
    #[error("Invalid length for community string. Task position: {task_idx}")]
    SnmpCommunityInvalidLength {
        task_idx: usize,
        min: usize,
        max: usize,
        provide: usize,
    },
    #[error("Invalid snmp-oid(pos: {pos}): {oid}. Task position: {task_idx}")]
    InvalidSnmpOid {
        task_idx: usize,
        oid: String,
        pos: usize,
    },
    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug, Clone)]
pub enum SnapShotError {
    #[error("Can`t update Snapshot(id={snapshot_id}). Worker with id: {worker_id} not found.")]
    UpdateWorkerNotFound {
        snapshot_id: String,
        worker_id: String,
    },
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
    #[error("{name} can`t be empty")]
    CantBeEmpty { name: String },
    #[error("{message}")]
    Common { message: String },
}

#[derive(Error, Debug, Clone)]
pub enum PollError {
    #[error("\nDetails:\nRetries: {}\nErrors: {}", 
        errors.len(),
        /*
        if let Some(name) = name {
            format!("Name: {} ", name)
        } else {
            "".to_string()
        },
*/
        errors.iter().enumerate().map(|(i, e)| {
            format!("{}: {e}", i + 1)
        }).collect::<Vec<String>>().join("\n")
    )]
    NoResponse {
        //target: String,
        //name: Option<String>,
        //retries: u8,
        errors: Vec<PollErrorContext>,
    },
    #[error("{message}")]
    Other { message: String },
}

#[derive(Error, Debug, Clone)]
pub enum SnmpError {
    #[error("SNMP error: timeout error for {target} with {retries} retries")]
    RequestTimeOut { target: SocketAddr, retries: u32 },
    #[error("Timeout while connecting to host {0}")]
    TimeOut(String),
    #[error("Host {0} is unreachable")]
    HostUnreachable(String),
    #[error("Invalid OID: {0}")]
    InvalidOid(String),
    #[error("Failed to connect to {target}:{port}.")]
    ConnectionFailed { target: IpAddr, port: u16 },
    #[error("Error parse raw SNMP value: {0}")]
    ParseRawValue(String),
    #[error("Internal SNMP error: {0}")]
    Internal(String),
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
