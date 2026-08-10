use std::net::IpAddr;

use crate::snmp::{SnmpQueryItem, primitives::Community};

#[derive(Debug)]
pub struct SnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
}

#[derive(Debug)]
pub struct QuerySnmpGet {
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItem>,
}
