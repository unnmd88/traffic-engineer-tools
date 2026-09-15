use crate::snmp::{
    builders::{to_stage_u405, to_stage_val_stcip, to_stage_val_swarco_8stages},
    oid_metadata::{AccessType, OidMetadata, Requirenment},
    parsers::{
        parse_ug405_stage, site_id_ug405_potok,
        stage_stcip::{parse_stcip_stage_potok, parse_stcip_stage_swarco},
    },
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
    builder: None,
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
    builder: None,
};

/// utcReplyGn for UTMC(for example)
pub const UTC_REPLY_GN_UTMC_METADATA: OidMetadata = OidMetadata {
    oid: UTC_REPLY_GN_OID,
    name: UTC_REPLY_GN_NAME,
    aliases: STAGE_ALIASES,
    description: UTC_REPLY_GN_DESCRIPTION,
    requires: None,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(parse_ug405_stage),
    builder: None,
};

/// swarcoUTCTrafftechPhaseStatus for Swarco
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    oid: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_OID,
    name: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME,
    aliases: STAGE_ALIASES,
    description: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION,
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::Unsigned32,
    requires: None,
    parser: Some(parse_stcip_stage_swarco),
    builder: Some(to_stage_val_swarco_8stages),
};

/// swarcoUTCTrafftechPhaseStatus for Potok
pub const POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    oid: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_OID,
    name: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME,
    aliases: STAGE_ALIASES,
    description: SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION,
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::Unsigned32,
    requires: None,
    parser: Some(parse_stcip_stage_potok),
    builder: Some(to_stage_val_stcip),
};

/// utcControlFn
pub const UTC_CONTROL_FN_METADATA: OidMetadata = OidMetadata {
    oid: UTC_CONTROL_FN_OID,
    name: UTC_CONTROL_FN_OID_NAME,
    aliases: STAGE_ALIASES,
    description: UTC_CONTROL_FN_OID_DESCRIPTION,
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::OctetString,
    requires: Some(&[Requirenment::Scn]),
    parser: Some(parse_ug405_stage),
    builder: Some(to_stage_u405),
};
