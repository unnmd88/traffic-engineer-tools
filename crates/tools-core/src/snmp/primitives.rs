use std::fmt;

use derive_more::{AsRef, Constructor, Deref, Display, From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{SnmpError, error::ParseError};

#[derive(Debug, Clone, Hash, Eq, PartialEq, AsRef, Deref, Display, Constructor)]
pub struct SnmpOid(async_snmp::Oid);

impl SnmpOid {
    pub fn parse(s: &str) -> Result<Self, SnmpError> {
        let inner = async_snmp::Oid::parse(s).map_err(|_| SnmpError::InvalidOid(s.to_string()))?;
        Ok(Self(inner))
    }
}

impl Serialize for SnmpOid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SnmpOid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        SnmpOid::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, AsRef, Deref, Constructor)]
pub struct SnmpRawValue(async_snmp::Value);

impl fmt::Display for SnmpRawValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            async_snmp::Value::Integer(v) => write!(f, "{} (Integer)", v),
            async_snmp::Value::Gauge32(v) => write!(f, "{} (Gauge32)", v),
            async_snmp::Value::Counter32(v) => write!(f, "{} (Counter32)", v),
            async_snmp::Value::Counter64(v) => write!(f, "{} (Counter64)", v),
            async_snmp::Value::OctetString(v) => match String::from_utf8(v.to_vec()) {
                Ok(s) => write!(f, "\"{}\" (OctetString)", s),
                Err(_) => write!(f, "{:?} (OctetString)", v),
            },
            async_snmp::Value::TimeTicks(v) => write!(f, "{} (TimeTicks)", v),
            async_snmp::Value::Null => write!(f, "Null"),
            async_snmp::Value::NoSuchObject => write!(f, "NoSuchObject"),
            async_snmp::Value::NoSuchInstance => write!(f, "NoSuchInstance"),
            async_snmp::Value::EndOfMibView => write!(f, "EndOfMibView"),
            _ => write!(f, "{:?}", self.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnmpVarbind {
    pub oid: SnmpOid,
    pub value: SnmpRawValue,
}

#[derive(
    Debug, Clone, Hash, Eq, PartialEq, From, Into, AsRef, Deref, Display, Serialize, Deserialize,
)]
pub struct Community(String);

impl Community {
    pub fn parse(value: String) -> Result<Self, ParseError> {
        if value.is_empty() {
            return Err(ParseError::CantBeEmpty {
                name: "community".to_string(),
            });
        }
        if value.len() > 32 {
            return Err(ParseError::InvalidLength {
                message: "community string too long.".to_string(),
                min: 1,
                max: 32,
                provided: value.len(),
            });
        }
        Ok(Self(value))
    }
}
#[derive(
    Debug, Clone, Hash, Eq, PartialEq, From, Into, AsRef, Deref, Display, Constructor, Serialize,
)]
pub struct Port(pub u16);
