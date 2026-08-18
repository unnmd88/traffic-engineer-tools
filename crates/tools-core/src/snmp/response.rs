use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct SnmpGetSample {
    pub oid_name: Option<String>,
    pub oid: String,
    pub value: Option<String>,
    pub raw_value: String,
}

impl Display for SnmpGetSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = self.oid_name.as_deref().unwrap_or("");
        let value = self.value.as_deref().unwrap_or("-");

        if name.is_empty() {
            // Example: OID[Name]: val: X raw: Y
            write!(f, "{}: {} raw val: {}\n", self.oid, value, self.raw_value)?;
        } else {
            // Example: OID: val: X raw: Y
            write!(f, "{}[{}]: {} raw val: {}\n", self.oid, name, value, self.raw_value)?;
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
