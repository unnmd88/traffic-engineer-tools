use crate::snmp::{
    oid_metadata::{AccessType, OidMetadata, Requirenment},
    parsers::{parse_ug405_stage, site_id_ug405_potok, stage_stcip::parse_stcip_stage},
    registry::STAGE_ALIASES,
    value::SnmpValueType,
};

use super::properties::*;

/// utcReplySiteID for Potok
pub const UTC_REPLY_SITE_ID_POTOK_METADATA: OidMetadata = OidMetadata {
    oid: UTC_REPLY_SITE_ID_POTOK_OID,
    name: UTC_REPLY_SITE_ID_POTOK_NAME,
    aliases: &[UTC_REPLY_SITE_ID_POTOK_ALIAS],
    description: UTC_REPLY_SITE_ID_POTOK_DESCRIPTION,
    requires: None,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(site_id_ug405_potok),
};

/// utcReplyGn for UG405 with Scn reqire
pub const UTC_REPLY_GN_UG405_METADATA: OidMetadata = OidMetadata {
    oid: UTC_REPLY_GN_OID,
    name: UTC_REPLY_GN_NAME,
    aliases: STAGE_ALIASES,
    description: UTC_REPLY_GN_DESCRIPTION,
    requires: Some(&[Requirenment::Scn]),
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(parse_ug405_stage),
};

/// utcReplyGn for UTMC
pub const UTC_REPLY_GN_UTMC_METADATA: OidMetadata = OidMetadata {
    oid: UTC_REPLY_GN_OID,
    name: UTC_REPLY_GN_NAME,
    aliases: STAGE_ALIASES,
    description: UTC_REPLY_GN_DESCRIPTION,
    requires: None,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(parse_ug405_stage),
};

/// swarcoUTCTrafftechPhaseStatus
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    oid: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_OID,
    name: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME,
    aliases: STAGE_ALIASES,
    description: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::Unsigned32,
    requires: None,
    parser: Some(parse_stcip_stage),
};
