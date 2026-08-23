use std::fmt::{self, format};

use crate::{
    domain::stage::Stage,
    error::ParseError,
    snmp::{parsers::bit_mask_ug405::parse_utc_bitmask, value::SnmpValue},
    utils::encode_to_hex,
};

pub struct Ug405Stage {
    as_bytes: Vec<u8>,
    stage: Stage,
}

impl Ug405Stage {
    pub fn from_oid_value(value: &SnmpValue) -> Result<Self, ParseError> {
        if !value.is_octet_string() {
            return Err(ParseError::InvalidType {
                expected: "OctetString".to_string(),
                actual: "-".to_string(),
            });
        }

        let bytes = to_bytes(value)?;
        let stage = Stage::new(parse_utc_bitmask(&bytes)?);

        Ok(Self {
            as_bytes: bytes,
            stage,
        })
    }

    pub fn as_hex_string(&self) -> String {
        encode_to_hex(&self.as_bytes)
    }

    pub fn stage(&self) -> &Stage {
        &self.stage
    }
}

impl fmt::Display for Ug405Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(0x{})", self.stage, self.as_hex_string())
    }
}

fn to_bytes(value: &SnmpValue) -> Result<Vec<u8>, ParseError> {
    let bytes = value.as_bytes().ok_or_else(|| {
        tracing::error!(
            target: "Ug405Stage::from_oid_value",
            "Bug! is_octet_string() returned true, but as_bytes() returned None"
        );
        ParseError::Common {
            message: "Can't parse bytes".to_string(),
        }
    })?;
    Ok(bytes.to_vec())
}
