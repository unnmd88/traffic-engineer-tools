use std::io::BufRead;

use crate::snmp::{
    profiles::POTOK_UG405_PROFILE,
    registry::oids::{UTC_CONTROL_FN_METADATA, UTC_CONTROL_TO_METADATA},
};

use memchr::memmem;

const UTC_CONTROL_TO: &[u8] = UTC_CONTROL_TO_METADATA.oid.as_bytes();
const UTC_CONTROL_FN: &[u8] = UTC_CONTROL_FN_METADATA.oid.as_bytes();
