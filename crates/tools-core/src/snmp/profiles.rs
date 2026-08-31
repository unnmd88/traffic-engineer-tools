use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::{
    AsciiError, Error, SnmpError,
    domain::ascii::Ascii,
    snmp::{
        SnmpReadClient,
        oid::SnmpOid,
        oid_metadata::{OidMetadata, Requirenment},
        parsers::{OidValueParserFn, parse_ug405_stage, site_id_ug405_potok},
        registry::oids::{
            SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA, UTC_REPLY_GN_UG405_METADATA,
            UTC_REPLY_SITE_ID_POTOK_METADATA,
        },
        site_id::fetch_site_id_potok_ug405,
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
    pub async fn get_scn(&self, client: &SnmpReadClient) -> Result<Option<Ascii>, SnmpError> {
        match self {
            Self::PotokUg405 => {
                let bytes = fetch_site_id_potok_ug405(client).await?;
                let ascii =
                    Ascii::from_bytes(&bytes).map_err(|e| SnmpError::ConvertScn(e.to_string()))?;
                Ok(Some(ascii))
            }
            _ => Ok(None),
        }
    }

    pub async fn resolve_oids(
        &self,
        client: &SnmpReadClient,
        to_resolve: &[SnmpOid],
    ) -> Result<Vec<SnmpOid>, SnmpError> {
        let needs_scn = self
            .registry()
            .iter()
            .any(|m| m.requires.is_some_and(|r| r.contains(&Requirenment::Scn)));

        if !needs_scn {
            return Ok(to_resolve.to_vec());
        }

        let scn = match self.get_scn(client).await? {
            Some(ascii) => ascii.to_scn(),
            None => {
                tracing::error!(target: "resolve_oids", profile=?self.to_string(), "Bug: snmp profile must have scn.");
                return Err(SnmpError::ScnError {
                    profile: self.to_string(),
                    message: "Scn not found".to_string(),
                });
            }
        };

        // 4. Приклеиваем SCN к нужным OID
        let mut result = Vec::with_capacity(to_resolve.len());
        for oid in to_resolve {
            let oid_str = oid.to_string();
            let metadata = self.registry().iter().find(|m| m.oid == oid_str);

            match metadata {
                Some(meta) => {
                    if meta
                        .requires
                        .is_some_and(|r| r.contains(&Requirenment::Scn))
                    {
                        let new_oid_str = format!("{}{}", oid_str, scn);
                        let resolved = SnmpOid::parse(&new_oid_str)
                            .map_err(|e| SnmpError::ResolveOid(e.to_string()))?;
                        result.push(resolved);
                    } else {
                        result.push(oid.clone());
                    }
                }
                None => {
                    result.push(oid.clone());
                }
            }
        }

        Ok(result)
    }

    pub fn registry(&self) -> &'static [OidMetadata] {
        match self {
            Self::Swarco => &[SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA],
            Self::PotokS => &[SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA],
            Self::PotokUg405 => &[
                UTC_REPLY_SITE_ID_POTOK_METADATA,
                UTC_REPLY_GN_UG405_METADATA,
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
