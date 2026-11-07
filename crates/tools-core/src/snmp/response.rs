use serde::{Deserialize, Serialize};

use crate::snmp::{business_value::BusinessValue, oid::SnmpOid, value::SnmpValue};

/// Результат одного опрошенного OID: распарсенное бизнес-значение + сырое.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnmpGetSample {
    pub oid_name: Option<String>,
    pub oid: SnmpOid,
    pub value: Option<BusinessValue>,
    pub raw_value: SnmpValue,
}
