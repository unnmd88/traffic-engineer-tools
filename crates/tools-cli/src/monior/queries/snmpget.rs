use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SnmpOidItemDto {
    #[serde(default)]
    pub name: Option<String>,
    pub oid: String,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnmpGetQueryDto {
    pub name: Option<String>,
    pub profile: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItemDto>,
}
