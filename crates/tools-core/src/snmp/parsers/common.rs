use crate::{
    snmp::ParseError,
    snmp::{business_value::BusinessValue, value::SnmpValue},
};

pub fn site_id_ug405_potok(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let bytes = value
        .as_bytes()
        .ok_or(ParseError::Common {
            message: "snmp-oid value is empty".to_string(),
        })?
        .to_vec();
    Ok(BusinessValue::SiteId { bytes })
}

pub fn as_i32(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = value.as_i32().ok_or(ParseError::Common {
        message: "parse to integer error".to_string(),
    })?;
    Ok(BusinessValue::Integer32(v))
}

pub fn as_u32(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = value.as_u32().ok_or(ParseError::Common {
        message: "parse to unsigned32 error".to_string(),
    })?;
    Ok(BusinessValue::Unsigned32(v))
}

pub fn as_u64(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = value.as_u64().ok_or(ParseError::Common {
        message: "parse to unsigned64 error".to_string(),
    })?;
    Ok(BusinessValue::Unsigned64(v))
}
