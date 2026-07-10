//! SNMP-примитивы.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SnmpPrimitive {
    String(String),
    OctetString(Vec<u8>),
    Integer(i32),
    Uinteger(u64),
    Null,
}

impl SnmpPrimitive {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::String(s) => s.as_bytes().to_vec(),
            Self::OctetString(bytes) => bytes.clone(),
            Self::Integer(i) => i.to_be_bytes().to_vec(),
            Self::Uinteger(u) => u.to_be_bytes().to_vec(),
            Self::Null => vec![],
        }
    }
}

impl fmt::Display for SnmpPrimitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "{}", s),
            Self::OctetString(bytes) => {
                for (i, byte) in bytes.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{:02X}", byte)?;
                }
                Ok(())
            }
            Self::Integer(i) => write!(f, "{}", i),
            Self::Uinteger(u) => write!(f, "{}", u),
            Self::Null => write!(f, "null"),
        }
    }
}
