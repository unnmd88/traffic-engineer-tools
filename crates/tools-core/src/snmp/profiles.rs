use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::snmp::{
    ParseError,
    oid::SnmpOid,
    oid_metadata::OidMetadata,
    registry::oids::{
        POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA, SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_METADATA,
        SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA, UTC_CONTROL_FN_METADATA,
        UTC_CONTROL_TO_METADATA, UTC_REPLY_GN_UG405_METADATA, UTC_REPLY_SITE_ID_POTOK_METADATA,
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
    pub fn get_metadata_by_name_or_alias(
        &self,
        name_or_alias: &str,
    ) -> Option<&'static OidMetadata> {
        let name_or_alias = name_or_alias.trim();

        for m in self.registry().iter() {
            if m.name.eq_ignore_ascii_case(&name_or_alias) {
                return Some(m);
            }

            if m.aliases.iter().any(|a| {
                a.eq_ignore_ascii_case(
                    &name_or_alias
                        .to_lowercase()
                        .replace(" ", "_")
                        .replace("-", "_"),
                )
            }) {
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

    /// Разрешает сырую строку (числовой OID или алиас) в OID этого профиля.
    pub fn resolve_oid(&self, raw: &str) -> Result<SnmpOid, ParseError> {
        let raw = raw.trim();

        if let Ok(oid) = SnmpOid::parse(&raw) {
            return Ok(oid);
        }

        let meta = self
            .get_metadata_by_name_or_alias(&raw)
            .ok_or(ParseError::UnknownAlias {
                alias: raw.to_string(),
            })?;

        SnmpOid::parse(meta.oid).map_err(|_| ParseError::Common {
            message: format!("invalid OID in profile registry: {}", meta.oid),
        })
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
                    assert!(seen.insert(key), "duplicate alias '{alias}' in profile {profile:?}");
                }
            }
        }
    }
}
