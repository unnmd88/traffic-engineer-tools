use derive_more::{AsRef, Constructor, Deref, Display};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::snmp::SnmpError;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Display, Constructor)]
pub struct SnmpOid(async_snmp::Oid);

impl Serialize for SnmpOid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SnmpOid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_uses_a_validated_oid_string() {
        let oid = SnmpOid::parse("1.3.6.1.2.1.1.1.0").unwrap();
        let json = serde_json::to_string(&oid).unwrap();
        assert_eq!(json, "\"1.3.6.1.2.1.1.1.0\"");
        assert_eq!(serde_json::from_str::<SnmpOid>(&json).unwrap(), oid);
        assert!(serde_json::from_str::<SnmpOid>("\"invalid oid\"").is_err());
    }
}
