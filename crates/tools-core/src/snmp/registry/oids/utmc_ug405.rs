use async_snmp::oid;

use crate::snmp::{
    oid::SnmpOid,
    oid_metadata::OidMetadata,
    parsers::{parse_ug405_stage, site_id_ug405_potok},
};

/// utcReplySiteID for Potok with index 0
pub const UTC_REPLY_SITE_ID_POTOK_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.2.0";
pub const UTC_REPLY_SITE_ID_POTOK_NAME: &str = "utcReplySiteID";
pub const UTC_REPLY_SITE_ID_POTOK_ALIAS: &str = "Scn";
pub const UTC_REPLY_SITE_ID_POTOK_DESCRIPTION: &str =
    " Identifies which, of possibly several, equipment at this site the
object should be applied to. The format is a Free Text ASCII String
Typically this could be an SCN, IP address, or a number. Potok use only index 0";
pub const UTC_REPLY_SITE_ID_POTOK_METADATA: OidMetadata = OidMetadata {
    name: UTC_REPLY_SITE_ID_POTOK_NAME,
    alias: UTC_REPLY_SITE_ID_POTOK_ALIAS,
    description: UTC_REPLY_SITE_ID_POTOK_DESCRIPTION,
    parser: Some(site_id_ug405_potok),
};
/*-----------------------------------------------------------------*/

/// utcReplyGn
pub const UTC_REPLY_GN_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.3";
pub const UTC_REPLY_GN_NAME: &str = "utcReplyGn";
pub const UTC_REPLY_GN_ALIAS: &str = "Stage";
pub const UTC_REPLY_GN_DESCRIPTION: &str =
    "Condition 1 confirms that a particular stage, or phase if specified is running.
G1 and G2 shall normally be returned simultaneously to indicate
                        
                        that one of the following has
                        occurred:
                        a) the mains supply to the signal
                        aspects is off;
                        b) manual method of traffic
                        control is either in operation or
                        requested;
                        c) The traffic controller is
                        switched off;
                        d) The traffic controller has failed
                        or shut down due to a fault;
                        e) The interface between the
                        OTU and the controller has
                        been disconnected.";
pub const UTC_REPLY_GN_METADATA: OidMetadata = OidMetadata {
    name: UTC_REPLY_GN_NAME,
    alias: UTC_REPLY_GN_ALIAS,
    description: UTC_REPLY_GN_DESCRIPTION,
    parser: Some(parse_ug405_stage),
};
/*-----------------------------------------------------------------*/

pub const UTC_CONTROL_SITE_ID: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.2";
pub const UTC_REPLY_SITE_ID: &str = "1.3.6.1.4.1.13267.3.2.5.1";

pub const UTC_CONTROL_SITE_ID_POTOK_OID: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.2.0";

pub const UTC_CONTROL_TO: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.15";

pub const SCN_REQUIRED_OIDS: [&str; 2] = [UTC_REPLY_GN_OID, UTC_CONTROL_TO];

pub const STAGE_ALIASES: [&str; 4] = ["stage", "phase", "фаза", "utcReplyGn"];

pub fn scn_required_for_oid_as_str(oid: &str) -> bool {
    SCN_REQUIRED_OIDS.contains(&oid)
}

pub fn scn_required(oid: &SnmpOid) -> bool {
    scn_required_for_oid_as_str(oid.to_string().as_ref())
}
