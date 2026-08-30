use crate::snmp::{
    oid_metadata::OidMetadata, parsers::stage_stcip::parse_stcip_stage, registry::oids::STAGE_ALIAS,
};

pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_OID: &str = "1.3.6.1.4.1.1618.3.7.2.11.2";
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME: &str = "swarcoUTCTrafftechPhaseStatus";
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_ALIAS: &str = STAGE_ALIAS;
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION: &str = "Current phase";

pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    name: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME,
    alias: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_ALIAS,
    description: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION,
    parser: Some(parse_stcip_stage),
};
