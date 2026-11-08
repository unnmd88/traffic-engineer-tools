use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::snmp::{
    ParseError,
    oid::SnmpOid,
    oid_metadata::{OidKind, OidMetadata},
    registry::oids::{
        POTOK_UG405_FIRMWARE_VERSION, POTOK_UG405_GROUPS_STATE_FROM1_TO66,
        POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA, SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_METADATA,
        SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA, UTC_CONTROL_FN_METADATA,
        UTC_CONTROL_TO_METADATA, UTC_REPLY_GN_UG405_METADATA, UTC_REPLY_SITE_ID_POTOK_METADATA,
        UTC_TYPE2_OPERATION_MODE_METADATA,
    },
};

pub const POTOK_UG405_PROFILE: SnmpProfile = SnmpProfile::PotokUg405;

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
                POTOK_UG405_FIRMWARE_VERSION,
                POTOK_UG405_GROUPS_STATE_FROM1_TO66,
            ],
            Self::PotokUtmc => &[],
            Self::SignalSxtp => &[],
            Self::PeekUg405 => &[],
        }
    }

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

    pub fn get_metadata_by_oid(&self, oid: &SnmpOid) -> Option<&'static OidMetadata> {
        let binding = oid.to_string();
        let oid_as_str = binding.as_str();

        let registry = self.registry();
        registry.iter().find(|m| m.oid == oid_as_str).or_else(|| {
            let definition = oid_as_str.strip_suffix(".0")?;
            registry
                .iter()
                .find(|m| m.kind == OidKind::Scalar && m.oid == definition)
        })
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
    use crate::snmp::{business_value::BusinessValue, oid::SnmpOid, value::SnmpValue};
    use std::collections::HashSet;

    #[test]
    fn site_id_parser_is_available_for_definition_and_scalar_instance() {
        for oid in [
            "1.3.6.1.4.1.13267.3.2.5.1.1.2",
            "1.3.6.1.4.1.13267.3.2.5.1.1.2.0",
            ".1.3.6.1.4.1.13267.3.2.5.1.1.2.0",
        ] {
            let oid = SnmpOid::parse(oid).unwrap();
            let metadata = SnmpProfile::PotokUg405.get_metadata_by_oid(&oid).unwrap();
            let parser = metadata.display.unwrap();
            let value = parser(&SnmpValue::OctetString(b"CO101".to_vec())).unwrap();
            assert!(matches!(value, BusinessValue::SiteId { bytes } if bytes == b"CO101"));
        }
    }

    #[test]
    fn scalar_metadata_does_not_match_other_instances_or_indexed_oids() {
        for oid in [
            "1.3.6.1.4.1.13267.3.2.5.1.1.2.1",
            "1.3.6.1.4.1.13267.3.2.5.1.1.2.0.0",
            "1.3.6.1.4.1.13267.3.2.5.1.1.3.0",
        ] {
            let oid = SnmpOid::parse(oid).unwrap();
            assert!(SnmpProfile::PotokUg405.get_metadata_by_oid(&oid).is_none());
        }
    }

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
