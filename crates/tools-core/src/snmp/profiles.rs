use std::{io::Seek, str::FromStr};

use itertools::Itertools;
use strum::{Display, EnumIter, IntoEnumIterator};

use crate::{
    AsciiError, Error, SnmpError,
    domain::ascii::Ascii,
    snmp::{
        SnmpReadClient,
        oid::SnmpOid,
        registry::{STAGE_ALIASES, utcReplyGn, utcReplySiteID},
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
    pub async fn get_scn(&self, client: &SnmpReadClient) -> Result<Option<Ascii>, SnmpError> {
        match self {
            Self::PotokUg405 => {
                let site_id_as_bytes = get_site_id_from_potok(client).await?;
                let site_id = Ascii::from_bytes(&site_id_as_bytes).map_err(|e| {
                    println!("Ошибка: {e}");
                    tracing::error!(target: "Get scn", "{}", e);
                    SnmpError::ConvertScn(e.to_string())
                })?;

                Ok(Some(site_id))
            }
            Self::PeekUg405 => {
                // TODO: реализовать логику для Peek
                // let scn = get_scn_from_peek(client).await?;
                // Ok(Some(scn))
                todo!("PeekUg405 SCN logic not implemented yet")
            }
            _ => Ok(None),
        }
    }

    /// Получить OID по алиасу для этого профиля
    pub fn get_oid_by_alias(&self, alias: &str) -> Option<&'static str> {
        match self {
            Self::PotokUg405 => STAGE_ALIASES.contains(&alias).then_some(utcReplyGn),
            // Остальные варианты
            _ => None,
        }
    }
}

pub async fn get_site_id_from_potok(client: &SnmpReadClient) -> Result<Vec<u8>, SnmpError> {
    let oid = SnmpOid::parse(utcReplySiteID)?;
    let varbind = client.get(&oid).await?;
    println!("OID: {oid:?}");

    varbind
        .value
        .as_bytes()
        .map(|b| b.to_vec())
        .ok_or_else(|| SnmpError::UnexpectedValueType {
            expected: "OctetString".to_string(),
            actual: varbind.value.as_string(),
        })
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
