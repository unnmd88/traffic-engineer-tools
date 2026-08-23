use core::fmt;
use std::fmt::{Display, Formatter};

use crate::snmp::{business_value::BusinessValue, oid::SnmpOid, value::SnmpValue};

#[derive(Debug, Clone)]
pub struct SnmpGetSample {
    pub oid_name: Option<String>,
    pub oid: SnmpOid,
    pub value: Option<BusinessValue>,
    pub raw_value: SnmpValue,
}

impl Display for SnmpGetSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = self.oid_name.as_deref().unwrap_or("");
        let raw_value = self.raw_value.as_string();
        let value = self
            .value
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());

        if name.is_empty() {
            // Example: OID[Name]: val: X raw: Y
            write!(f, "{}: {value} raw val: {raw_value}\n", self.oid,)?;
        } else {
            // Example: OID: val: X raw: Y
            write!(f, "{}[{name}]: {value} raw val: {raw_value}\n", self.oid,)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SnmpGetResponse {
    pub samples: Vec<SnmpGetSample>,
}

impl Display for SnmpGetResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "OIDs({}):", self.samples.len())?;
        for sample in self.samples.iter() {
            write!(f, " - {sample}")?
        }

        Ok(())
    }
}
