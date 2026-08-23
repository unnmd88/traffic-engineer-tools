use serde::{Deserialize, Serialize};

use crate::snmp::{oid::SnmpOid, parsers::OidValueParserFn};

#[derive(Debug)]
pub struct SnmpGetQueryItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
    pub business_value_parser: Option<OidValueParserFn>,
}
