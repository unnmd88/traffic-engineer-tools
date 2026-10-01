use crate::snmp::{
    ParseError, business_value::BusinessValue, group_state_potok_ug405::GroupStatePotokUg405,
    value::SnmpValue,
};

fn group_state_potok_ug405(
    value: &SnmpValue,
    start_group: u8,
) -> Result<BusinessValue, ParseError> {
    let bytes = value.as_bytes().ok_or(ParseError::Common {
        message: "snmp-oid value is empty".to_string(),
    })?;

    let mut groups = Vec::with_capacity(bytes.len());
    let mut num_group = start_group;
    for b in bytes {
        groups.push(GroupStatePotokUg405::from_byte(num_group, *b));
        num_group += 1;
    }

    Ok(BusinessValue::GroupStatePotokUg405 { groups })
}

pub fn group_state_from1_to66_potok_ug405(value: &SnmpValue) -> Result<BusinessValue, ParseError> {
    group_state_potok_ug405(value, 1)
}
