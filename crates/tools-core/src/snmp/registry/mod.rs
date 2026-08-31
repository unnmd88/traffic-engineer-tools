use std::collections::HashMap;

use async_snmp::Oid;
use derive_more::{Constructor, Display};

use crate::{Error, snmp::oid::SnmpOid};
pub mod oids;
pub use oids::STAGE_ALIASES;
//pub use oids::stcip::*;
//pub use oids::utmc_ug405::*;

pub const swarcoUTCDetectorQtyOid: &str = ".1.3.6.1.4.1.1618.3.3.2.2.2.0";
pub const swarcoUTCTrafftechPlanCurrentOid: &str = ".1.3.6.1.4.1.1618.3.7.2.1.2.0";

pub const swarcoUTCDetectorQty: &str = "swarcoUTCDetectorQty";
