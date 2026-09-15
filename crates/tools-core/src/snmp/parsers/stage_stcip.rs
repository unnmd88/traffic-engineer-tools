use anyhow::Result;
use async_snmp::value;

use crate::{
    snmp::ParseError,
    snmp::{
        business_value::BusinessValue,
        profiles::SnmpProfile,
        value::{SnmpValue, SnmpValueType},
    },
    stage::Stage,
};

fn parse_num(v: &SnmpValue) -> Result<u32, ParseError> {
    v.as_u32().ok_or_else(|| ParseError::InvalidType {
        expected: "Unsigned32".to_string(),
        actual: SnmpValueType::from(v).to_string(),
    })
}

pub fn parse_stcip_stage_swarco(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = parse_num(value)?;

    let stage = match v {
        1 => 8,
        _ => v - 1,
    };

    Ok(BusinessValue::Stage(Stage::new(stage)))
}

pub fn parse_stcip_stage_potok(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let v = parse_num(value)?;

    Ok(BusinessValue::Stage(Stage::new(v - 1)))
}
