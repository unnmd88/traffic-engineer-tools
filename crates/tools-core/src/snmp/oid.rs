use derive_more::{AsRef, Constructor, Deref, Display};

use crate::snmp::SnmpError;

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

    pub fn child(&self, arc: u32) -> Self {
        Self(self.0.child(arc))
    }

    pub fn with_suffix(&self, suffix: &SnmpOid) -> Self {
        let arcs = self
            .0
            .arcs()
            .iter()
            .copied()
            .chain(suffix.0.arcs().iter().copied());
        Self(async_snmp::Oid::new(arcs))
    }
}
