use serde::{Deserialize, Serialize};

use crate::snmp::{parsers::OidValueParserFn, primitives::SnmpOid};

#[derive(Debug)]
pub struct SnmpQueryItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
    pub parser: Option<OidValueParserFn>,
}
