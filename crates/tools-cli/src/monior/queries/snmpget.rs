
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SnmpOidItemDto {
    pub name: Option<String>,
    pub oid: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnmpGetQueryDto {
    pub name: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItemDto>,
}
