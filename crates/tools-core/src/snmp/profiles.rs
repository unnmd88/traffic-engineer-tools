use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::{
    AsciiError, Error, SnmpError,
    domain::ascii::Ascii,
    snmp::{
        SnmpReadClient,
        oid::SnmpOid,
        oid_metadata::OidMetadata,
        parsers::{OidValueParserFn, parse_ug405_stage, site_id_ug405_potok},
        registry::{
            STAGE_ALIASES, UTC_REPLY_GN_METADATA, UTC_REPLY_GN_OID,
            UTC_REPLY_SITE_ID_POTOK_METADATA, UTC_REPLY_SITE_ID_POTOK_OID,
        },
        site_id::fetch_site_id_potok_ug405,
    },
};

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
    pub fn required_scn(&self) -> bool {
        matches!(self, Self::PotokUg405 | Self::PeekUg405)
    }

    pub async fn get_site_id_if_required(
        &self,
        client: &SnmpReadClient,
    ) -> Result<Option<Vec<u8>>, SnmpError> {
        let site_id = match self {
            Self::PotokUg405 => Some(fetch_site_id_potok_ug405(client).await?),
            _ => None,
        };

        Ok(site_id)
    }

    /// Получить OID по алиасу для этого профиля
    pub fn get_oid_by_alias(&self, alias: &str) -> Option<&'static str> {
        match self {
            Self::PotokUg405 => STAGE_ALIASES.contains(&alias).then_some(UTC_REPLY_GN_OID),
            // Остальные варианты
            _ => None,
        }
    }

    pub fn metadata(&self, oid: &SnmpOid) -> Option<OidMetadata> {
        let binding = oid.to_string();
        let oid_as_str = binding.as_str();

        match self {
            Self::PotokUg405 => match oid_as_str {
                UTC_REPLY_GN_OID => Some(UTC_REPLY_GN_METADATA),
                UTC_REPLY_SITE_ID_POTOK_OID => Some(UTC_REPLY_SITE_ID_POTOK_METADATA),
                _ => None,
            },
            _ => None,
        }
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
