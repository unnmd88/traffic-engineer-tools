use std::fmt::{self, Formatter};

use derive_more::Display;

use crate::{SnmpError, snmp::oid::SnmpOid, utils::encode_to_hex};

#[derive(Debug, Clone, Display)]
pub enum SnmpValueType {
    OctetString,
    Opaque,
    Gauge32,
    Integer,
    Unsigned32,
    Counter32,
    Counter64,
    TimeTicks,
    Oid,
    IpAddress,
    Null,
    NoSuchObject,
    NoSuchInstance,
    EndOfMibView,
    Other,
    Unknown,
}

impl From<&SnmpValue> for SnmpValueType {
    fn from(value: &SnmpValue) -> Self {
        match value {
            SnmpValue::Integer(_) => Self::Integer,
            SnmpValue::Gauge32(_) => Self::Gauge32,
            SnmpValue::OctetString(_) => Self::OctetString,
            SnmpValue::Opaque(_) => Self::Opaque,
            SnmpValue::TimeTicks(_) => Self::TimeTicks,
            SnmpValue::Oid(_) => Self::Oid,
            SnmpValue::IpAddress(_) => Self::IpAddress,
            SnmpValue::Null => Self::Null,
            SnmpValue::NoSuchObject => Self::NoSuchObject,
            SnmpValue::NoSuchInstance => Self::NoSuchInstance,
            SnmpValue::EndOfMibView => Self::EndOfMibView,
            SnmpValue::Unsigned32(_) => Self::Unsigned32,
            SnmpValue::Counter32(_) => Self::Counter32,
            SnmpValue::Counter64(_) => Self::Counter64,
            SnmpValue::Unknown { .. } => Self::Unknown,
            SnmpValue::Other(_) => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SnmpValue {
    OctetString(Vec<u8>),
    Opaque(Vec<u8>),
    Gauge32(u32),
    Integer(i32),
    Unsigned32(u32),
    Counter32(u32),
    Counter64(u64),
    TimeTicks(u32),
    Oid(SnmpOid),
    IpAddress([u8; 4]),
    Null,
    NoSuchObject,
    NoSuchInstance,
    EndOfMibView,
    Other(String),
    Unknown { tag: u8, data: Vec<u8> },
}

impl SnmpValue {
    pub fn is_octet_string(&self) -> bool {
        matches!(self, SnmpValue::OctetString(_))
    }

    pub fn is_opaque(&self) -> bool {
        matches!(self, SnmpValue::Opaque(_))
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            SnmpValue::OctetString(v) => Some(v),
            SnmpValue::Opaque(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Self::Counter32(v) | Self::Gauge32(v) | Self::TimeTicks(v) => Some(*v),
            Self::Integer(v) if *v >= 0 => Some(*v as u32),
            _ => None,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            SnmpValue::Integer(v) => v.to_string(),
            SnmpValue::Gauge32(v) => v.to_string(),
            SnmpValue::OctetString(v) => format!("[{}]", encode_to_hex(v)),
            SnmpValue::Opaque(v) => format!("[{}]", encode_to_hex(v)),
            SnmpValue::TimeTicks(v) => v.to_string(),
            SnmpValue::Oid(v) => v.to_string(),
            SnmpValue::IpAddress(v) => format!("{}.{}.{}.{}", v[0], v[1], v[2], v[3]),
            SnmpValue::Null => "Null".to_string(),
            SnmpValue::NoSuchObject => "NoSuchObject".to_string(),
            SnmpValue::NoSuchInstance => "NoSuchInstance".to_string(),
            SnmpValue::EndOfMibView => "EndOfMibView".to_string(),
            SnmpValue::Unsigned32(v) => v.to_string(),
            SnmpValue::Counter32(v) => v.to_string(),
            SnmpValue::Counter64(v) => v.to_string(),
            SnmpValue::Unknown { tag, data } => format!("tag: {tag} data: {data:?}"),
            SnmpValue::Other(v) => v.clone(),
        }
    }
}

impl fmt::Display for SnmpValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SnmpValue::Integer(v) => write!(f, "{v}(Integer)"),
            SnmpValue::Gauge32(v) => write!(f, "{v}(Gauge32)"),
            SnmpValue::OctetString(v) => write!(f, "{}(OctetString)", encode_to_hex(v)),
            SnmpValue::Opaque(v) => write!(f, "{}(Opaque)", encode_to_hex(v)),
            SnmpValue::TimeTicks(v) => write!(f, "{v}(TimeTicks)"),
            SnmpValue::Oid(v) => write!(f, "{v}(Oid)"),
            SnmpValue::IpAddress(v) => write!(f, "{}.{}.{}.{}(IpAddress)", v[0], v[1], v[2], v[3]),
            SnmpValue::Null => write!(f, "Null"),
            SnmpValue::NoSuchObject => write!(f, "NoSuchObject"),
            SnmpValue::NoSuchInstance => write!(f, "NoSuchInstance"),
            SnmpValue::EndOfMibView => write!(f, "EndOfMibView"),
            SnmpValue::Unsigned32(v) => write!(f, "{v}(Unsigned32)"),
            SnmpValue::Counter32(v) => write!(f, "{v}(Counter32)"),
            SnmpValue::Counter64(v) => write!(f, "{v}(Counter64)"),
            SnmpValue::Unknown { tag, data } => write!(f, "tag: {tag} data: {data:?}"),
            SnmpValue::Other(v) => write!(f, "{v} (Other)"),
        }
    }
}

impl From<&async_snmp::Value> for SnmpValue {
    fn from(value: &async_snmp::Value) -> Self {
        match value {
            async_snmp::Value::Integer(v) => SnmpValue::Integer(*v),
            async_snmp::Value::Counter32(v) => SnmpValue::Counter32(*v),
            async_snmp::Value::Gauge32(v) => SnmpValue::Gauge32(*v),
            async_snmp::Value::Counter64(v) => SnmpValue::Counter64(*v),
            async_snmp::Value::OctetString(v) => SnmpValue::OctetString(v.to_vec()),
            async_snmp::Value::ObjectIdentifier(v) => SnmpValue::Oid(SnmpOid::new(v.clone())),
            async_snmp::Value::IpAddress(v) => SnmpValue::IpAddress(*v),
            async_snmp::Value::Null => SnmpValue::Null,
            async_snmp::Value::NoSuchObject => SnmpValue::NoSuchObject,
            async_snmp::Value::NoSuchInstance => SnmpValue::NoSuchInstance,
            async_snmp::Value::EndOfMibView => SnmpValue::EndOfMibView,
            _ => SnmpValue::Other(value.to_string()),
        }
    }
}

impl TryFrom<&SnmpValue> for async_snmp::Value {
    type Error = SnmpError;

    fn try_from(value: &SnmpValue) -> Result<Self, Self::Error> {
        match value {
            SnmpValue::Integer(v) => Ok(async_snmp::Value::Integer(*v)),
            SnmpValue::Unsigned32(v) => Ok(async_snmp::Value::Gauge32(*v)),
            SnmpValue::Counter32(v) => Ok(async_snmp::Value::Counter32(*v)),
            SnmpValue::Counter64(v) => Ok(async_snmp::Value::Counter64(*v)),
            SnmpValue::OctetString(v) => Ok(async_snmp::Value::OctetString(v.clone().into())),
            SnmpValue::Oid(v) => Ok(async_snmp::Value::ObjectIdentifier(v.inner().clone())),
            SnmpValue::IpAddress(v) => Ok(async_snmp::Value::IpAddress(*v)),
            SnmpValue::Null => Ok(async_snmp::Value::Null),
            _ => Err(SnmpError::UnsupportedForSet {
                value: value.as_string(),
            }),
        }
    }
}
