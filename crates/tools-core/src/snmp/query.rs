use serde::{Deserialize, Serialize};

use crate::snmp::{parsers::SnmpRawValueParserFn, primitives::SnmpOid};

#[derive(Debug)]
pub struct SnmpQueryItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
}
