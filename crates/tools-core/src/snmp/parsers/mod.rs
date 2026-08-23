use crate::{
    error::ParseError,
    snmp::{business_value::BusinessValue, value::SnmpValue},
};

mod base;

pub mod bit_mask_ug405;
pub mod stage_ug405;
pub use stage_ug405::parse_ug405_stage;

pub type OidValueParserFn = fn(&SnmpValue) -> Result<BusinessValue, ParseError>;
