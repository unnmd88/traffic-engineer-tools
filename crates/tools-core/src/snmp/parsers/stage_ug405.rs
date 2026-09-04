use crate::{
    error::ParseError,
    snmp::{business_value::BusinessValue, ug405_stage::Ug405Stage, value::SnmpValue},
};

pub fn parse_ug405_stage(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    let stage = Ug405Stage::from_oid_value(value)?;
    let as_hex = stage.as_hex_string();
    Ok(BusinessValue::StageUg405 {
        number: stage.stage().clone(),
        hex: as_hex,
    })
}
