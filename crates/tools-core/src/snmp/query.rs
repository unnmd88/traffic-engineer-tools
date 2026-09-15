use serde::{Deserialize, Serialize};

use crate::snmp::{oid::SnmpOid, parsers::OidValueParserFn, value::SnmpValue};

#[derive(Debug)]
pub struct SnmpGetQueryItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
    pub business_value_parser: Option<OidValueParserFn>,
}

/// Полностью зарезолвленный SET-элемент: OID и значение уже готовы к отправке.
#[derive(Debug, Clone)]
pub struct SnmpSetItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
    pub value: SnmpValue,
}
