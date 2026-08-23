use crate::snmp::{oid::SnmpOid, value::SnmpValue};

#[derive(Debug, Clone)]
pub struct SnmpVarbind {
    pub oid: SnmpOid,
    pub value: SnmpValue,
}
