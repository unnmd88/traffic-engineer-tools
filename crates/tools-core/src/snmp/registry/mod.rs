use std::collections::HashMap;

use async_snmp::Oid;
use derive_more::{Constructor, Display};

use crate::{Error, snmp::oid::SnmpOid};
mod oids;
pub use oids::STAGE_ALIASES;
pub use oids::stcip::*;
pub use oids::utmc_ug405::*;

pub const swarcoUTCDetectorQtyOid: &str = ".1.3.6.1.4.1.1618.3.3.2.2.2.0";
pub const swarcoUTCTrafftechPlanCurrentOid: &str = ".1.3.6.1.4.1.1618.3.7.2.1.2.0";

pub const swarcoUTCDetectorQty: &str = "swarcoUTCDetectorQty";

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueType {
    Integer,
    Unsigned32,
    OctetString,
    Gauge32,
}

pub struct OidDefinition {
    name: &'static str,
    description: &'static str,
    scn_required: bool,
    value_type: ValueType,
    //vendor: Vendor,
}

pub struct Registry {
    map: HashMap<SnmpOid, OidDefinition>,
}

impl Registry {
    pub fn empty() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn with_standard_oids() -> Result<Self, Error> {
        let mut map = HashMap::new();

        map.insert(
            SnmpOid::parse(swarcoUTCDetectorQtyOid)?,
            OidDefinition {
                name: swarcoUTCDetectorQty,
                description: "План",
                scn_required: false,
                value_type: ValueType::Unsigned32,
            },
        );

        // Другие оиды

        Ok(Self { map })
    }

    pub fn register(&mut self, oid: SnmpOid, metadata: OidDefinition) {
        self.map.insert(oid, metadata);
    }
}
