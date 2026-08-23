use derive_more::{AsRef, Constructor, Deref, Display};

use crate::SnmpError;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Display, Constructor)]
pub struct SnmpOid(async_snmp::Oid);

impl SnmpOid {
    pub fn parse(s: &str) -> Result<Self, SnmpError> {
        let inner = async_snmp::Oid::parse(s).map_err(|_| SnmpError::InvalidOid(s.to_string()))?;
        Ok(Self(inner))
    }

    pub fn inner(&self) -> &async_snmp::Oid {
        &self.0
    }
}
