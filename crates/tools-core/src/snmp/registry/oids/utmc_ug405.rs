use async_snmp::oid;

use crate::snmp::oid::SnmpOid;

pub const utcControlSiteID: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.2.0";
pub const utcReplySiteID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.2.0";

pub const utcReplyGn: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.3";
pub const utcControlTO: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.15";

pub const SCN_REQUIRED_OIDS: [&str; 2] = [utcReplyGn, utcControlTO];

pub const STAGE_ALIASES: [&str; 4] = ["stage", "phase", "фаза", "utcReplyGn"];

pub fn scn_required_for_oid_as_str(oid: &str) -> bool {
    SCN_REQUIRED_OIDS.contains(&oid)
}

pub fn scn_required(oid: &SnmpOid) -> bool {
    scn_required_for_oid_as_str(oid.to_string().as_ref())
}
