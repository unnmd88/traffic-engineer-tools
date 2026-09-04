use async_snmp::value;

use crate::{
    domain::stage::Stage,
    error::ParseError,
    snmp::{
        business_value::BusinessValue,
        profiles::SnmpProfile,
        value::{SnmpValue, SnmpValueType},
    },
};

pub fn parse_stcip_stage(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = value.as_u32().ok_or_else(|| ParseError::InvalidType {
        expected: "Unsigned32".to_string(),
        actual: SnmpValueType::from(value).to_string(),
    })?;

    let stage = match v {
        0 => {
            return Err(ParseError::Common {
                message: "Value can`t be 0.".to_string(),
            });
        }
        1 => 8,
        _ => v - 1,
    };

    Ok(BusinessValue::Stage(Stage::new(stage)))
}
