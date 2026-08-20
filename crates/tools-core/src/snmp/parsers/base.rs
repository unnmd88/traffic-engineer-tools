use crate::{
    error::SnmpError,
    snmp::primitives::{SnmpOid, SnmpRawValue},
};
use async_snmp::Oid;

pub type OidValueParserFn = fn(&SnmpRawValue) -> Result<String, SnmpError>;

pub fn default_parse(value: &SnmpRawValue) -> Result<String, SnmpError> {
    Ok(format!("{value}"))
}

pub fn debug_parse(value: &SnmpRawValue) -> Result<String, SnmpError> {
    Ok(format!("value: {value}  as bytes: {:?}", value.as_bytes()))
}

pub trait SnmpRawValueParser: Send + Sync {
    fn parse(&self, raw_value: &SnmpRawValue) -> Result<String, SnmpError>;
}

pub struct DefaultSnmpRawValueParser;

impl SnmpRawValueParser for DefaultSnmpRawValueParser {
    fn parse(&self, raw_value: &SnmpRawValue) -> Result<String, SnmpError> {
        Ok(format!("{:?}", raw_value))
    }
}

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
