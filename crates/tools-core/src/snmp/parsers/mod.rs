use crate::{
    snmp::ParseError,
    snmp::{business_value::BusinessValue, value::SnmpValue},
};

mod base;

pub mod bit_mask_ug405;
mod common;
pub mod stage_ug405;
pub use common::{as_i32, as_u32, as_u64, octet_string_as_utf8_lossy, site_id_ug405_potok};
pub use stage_ug405::parse_ug405_stage;
pub mod group_state_potok;
pub mod stage_stcip;

pub type OidValueDisplayFn = fn(&SnmpValue) -> Result<BusinessValue, ParseError>;
