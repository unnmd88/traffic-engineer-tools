use crate::{
    error::{ParseError, SnmpError},
    snmp::{business_value::BusinessValue, oid::SnmpOid, value::SnmpValue},
};
use async_snmp::Oid;

/// Парсит строковые OID в SnmpOid.
pub fn parse_oids(oids: &[String]) -> Result<Vec<SnmpOid>, SnmpError> {
    oids.iter()
        .map(|s| {
            s.parse::<Oid>()
                .map(|o| SnmpOid::new(o))
                .map_err(|_| SnmpError::InvalidOid(s.clone()))
        })
        .collect()
}
