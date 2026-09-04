use std::net::IpAddr;

use crate::snmp::{SnmpGetQueryItem, profiles::SnmpProfile};

#[derive(Debug, Clone)]
pub struct SnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QuerySnmpGet {
    pub profile: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItem>,
}
