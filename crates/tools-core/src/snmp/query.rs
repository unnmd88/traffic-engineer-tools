use serde::{Deserialize, Serialize};

use crate::snmp::primitives::SnmpOid;

#[derive(Debug, Serialize, Deserialize)]
pub struct SnmpQueryItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
}
