use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumIter)]
pub enum SnmpProfile {
    Swarco,
    PotokS,
    PotokUg405,
    PotokUtmc,
    PeekUg405,
    SignalSxtp,
}

impl SnmpProfile {
    pub fn scn_required(&self) -> bool {
        matches!(self, SnmpProfile::PotokUg405 | SnmpProfile::PeekUg405)
    }
}

impl FromStr for SnmpProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cleaned = s.trim().to_lowercase();

        match cleaned.as_str() {
            "swarco" => Ok(Self::Swarco),
            "potok_s" => Ok(Self::PotokS),
            "potok_ug405" => Ok(Self::PotokUg405),
            "potok_utmc" => Ok(Self::PotokUtmc),
            "peek" => Ok(Self::PeekUg405),
            "signal_sxtp" => Ok(Self::SignalSxtp),
            _ => Err(format!(
                "Unknown profile: '{s}'. Available: 'swarco', 'potok_s', 'potok_ug405', 'potok_utmc', 'peek', 'signal_sxtp'",
            )),
        }
    }
}
