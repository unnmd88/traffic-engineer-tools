use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct SnmpGetSample {
    pub oid_name: Option<String>,
    pub oid: String,
    pub raw_value: String,
}

impl Display for SnmpGetSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let oid = match &self.oid_name {
            Some(oid_name) => {
                writeln!(f, "{}[{oid_name}]: {}", &self.oid, &self.raw_value)?;
            }
            None => {
                writeln!(f, "{}: {}", &self.oid, &self.raw_value)?;
            }
        };

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
