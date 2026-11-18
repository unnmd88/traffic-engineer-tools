use serde::{Deserialize, Serialize};

use crate::snmp::{oid::SnmpOid, value::SnmpValue};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnmpVarbind {
    pub oid: SnmpOid,
    pub value: SnmpValue,
}
