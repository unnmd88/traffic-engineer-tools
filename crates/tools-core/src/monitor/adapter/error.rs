use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum AdapterBuildError {
    #[error("can't create snmp-driver, try again later")]
    SnmpClientCreate,
    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug, Clone)]
pub enum SnmpQueryError {
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
    SnmpCommunityInvalidLength {
        min: usize,
        max: usize,
        provide: usize,
    },
    #[error("invalid snmp-oid(pos: {pos}): {oid}")]
    InvalidSnmpOid { pos: usize, oid: String },
    #[error("{0}")]
    Other(String),
}
