use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::snmp::{
    oid::SnmpOid,
    oid_metadata::OidMetadata,
    registry::oids::{
        POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA, SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_METADATA,
        SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA, UTC_CONTROL_FN_METADATA, UTC_CONTROL_TO_METADATA,
        UTC_REPLY_GN_UG405_METADATA, UTC_REPLY_SITE_ID_POTOK_METADATA,
        UTC_TYPE2_OPERATION_MODE_METADATA,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Display)]
pub enum SnmpProfile {
    Swarco,
    PotokS,
    PotokUg405,
    PotokUtmc,
    PeekUg405,
    SignalSxtp,
}

impl SnmpProfile {
    pub fn registry(&self) -> &'static [OidMetadata] {
        match self {
            Self::Swarco => &[
                SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA,
                SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_METADATA,
            ],
            Self::PotokS => &[POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA],
            Self::PotokUg405 => &[
                UTC_REPLY_SITE_ID_POTOK_METADATA,
                UTC_REPLY_GN_UG405_METADATA,
                UTC_TYPE2_OPERATION_MODE_METADATA,
                UTC_CONTROL_TO_METADATA,
                UTC_CONTROL_FN_METADATA,
            ],
            Self::PotokUtmc => &[],
            Self::SignalSxtp => &[],
            Self::PeekUg405 => &[],
        }
    }

    /// Получить OidMetadata по алиасу для этого профиля
    pub fn get_metadata_by_name_or_alias(&self, alias: &str) -> Option<&'static OidMetadata> {
        let alias_lower = alias.to_lowercase();

        for m in self.registry().iter() {
            if m.name.eq_ignore_ascii_case(&alias_lower) {
                return Some(m);
            }

            if m.aliases
                .iter()
                .any(|a| a.eq_ignore_ascii_case(&alias_lower))
            {
                return Some(m);
            }
        }

        None
    }

    /// Получить OidMetadata по SnmpOid для этого профиля
    pub fn get_metadata_by_oid(&self, oid: &SnmpOid) -> Option<&'static OidMetadata> {
        let binding = oid.to_string();
        let oid_as_str = binding.as_str();

        self.registry().iter().find(|m| m.oid == oid_as_str)
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

#[cfg(test)]
mod tests {
    use super::SnmpProfile;
    use std::collections::HashSet;

    #[test]
    fn aliases_are_unique_within_profile() {
        let profiles = [
            SnmpProfile::Swarco,
            SnmpProfile::PotokS,
            SnmpProfile::PotokUg405,
            SnmpProfile::PotokUtmc,
            SnmpProfile::PeekUg405,
            SnmpProfile::SignalSxtp,
        ];

        for profile in profiles {
            let mut seen: HashSet<String> = HashSet::new();
            for meta in profile.registry() {
                for alias in meta.aliases {
                    let key = alias.trim().to_lowercase();
                    assert!(
                        seen.insert(key),
                        "duplicate alias '{alias}' in profile {profile:?}"
                    );
                }
            }
        }
    }
}
