use crate::{
    error::ParseError,
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
