use std::net::IpAddr;

use crate::snmp::{
    ParseError, SnmpSetItem,
    builders::encode_by_type,
    community::Community,
    oid::SnmpOid,
    profiles::SnmpProfile,
    value::{SnmpValue, SnmpValueType},
};

use super::error::SnmpQueryError;

/// Валидированный OID-элемент запроса (`oid` уже распарсен из строки/алиаса).
#[derive(Debug, Clone)]
pub struct SnmpOidItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
}

/// Сырое описание OID из конфига (до валидации алиасов).
#[derive(Debug, Clone)]
pub struct RawSnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
}

/// Сырое описание SET-элемента из конфига.
#[derive(Debug, Clone)]
pub struct RawSnmpSetItem {
    pub name: Option<String>,
    pub oid: String,
    pub value: String,
    pub value_type: Option<String>,
}

/// Валидированный SNMP GET запрос: доменные типы вместо сырых строк.
#[derive(Debug, Clone)]
pub struct SnmpGetQuery {
    pub profile: Option<SnmpProfile>,
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub oids: Vec<SnmpOidItem>,
}

/// Валидированный SNMP SET запрос. OID и value полностью зарезолвлены.
#[derive(Debug, Clone)]
pub struct SnmpSetQuery {
    pub profile: Option<SnmpProfile>,
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub community_r: Option<Community>,
    pub sets: Vec<SnmpSetItem>,
}

/// Запрос задачи. Валидация сырых значений происходит при построении
#[derive(Clone, Debug)]
pub enum Query {
    SnmpGet(SnmpGetQuery),
    SnmpSet(SnmpSetQuery),
    // HttpRead(QueryHttpRead),
}

impl Query {
    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(q) => format!("{}:{}", q.host, q.port),
            Self::SnmpSet(q) => format!("{}:{}", q.host, q.port),
        }
    }
}

impl SnmpGetQuery {
    /// Собрать валидированный SNMP GET запрос из сырых значений конфига.
    pub fn from_raw(
        host: String,
        port: u16,
        community: String,
        profile: Option<String>,
        oids: Vec<RawSnmpOidItem>,
    ) -> Result<Self, SnmpQueryError> {
        let host = parse_ip(&host)?;
        let community = parse_community(&community)?;
        let profile = parse_profile(profile)?;

        let oids = oids
            .into_iter()
            .enumerate()
            .map(|(pos, raw)| {
                let oid = resolve_oid(&raw.oid, profile.as_ref(), pos)?;
                Ok(SnmpOidItem {
                    name: raw.name,
                    oid,
                })
            })
            .collect::<Result<Vec<_>, SnmpQueryError>>()?;

        Ok(Self {
            profile,
            host,
            port,
            community,
            oids,
        })
    }
}

impl SnmpSetQuery {
    /// Собрать валидированный SNMP SET запрос из сырых значений конфига.
    pub fn from_raw(
        host: String,
        port: u16,
        community_r: Option<String>,
        community_w: String,
        profile: Option<String>,
        sets: Vec<RawSnmpSetItem>,
    ) -> Result<Self, SnmpQueryError> {
        let host = parse_ip(&host)?;
        let community = parse_community(&community_w)?;
        let community_r = match community_r.as_ref() {
            Some(c) => Some(parse_community(c)?),
            None => None,
        };
        let profile = parse_profile(profile)?;

        let sets = sets
            .into_iter()
            .enumerate()
            .map(|(pos, raw)| {
                let oid = resolve_oid(&raw.oid, profile.as_ref(), pos)?;
                let value = encode_set_value(&oid, &raw.value, raw.value_type, profile.as_ref())?;
                Ok(SnmpSetItem {
                    name: raw.name,
                    oid,
                    value,
                })
            })
            .collect::<Result<Vec<_>, SnmpQueryError>>()?;

        Ok(Self {
            profile,
            host,
            port,
            community,
            community_r,
            sets,
        })
    }
}

fn parse_ip(ip: &str) -> Result<IpAddr, SnmpQueryError> {
    ip.parse::<IpAddr>()
        .map_err(|_| SnmpQueryError::InvalidIpAddress { ip: ip.to_string() })
}

fn parse_community(community: &str) -> Result<Community, SnmpQueryError> {
    tracing::debug!(target: "parse_community", "r={}", community);
    Community::parse(community.to_string()).map_err(|e| match e {
        ParseError::CantBeEmpty { .. } => SnmpQueryError::SnmpCommunityIsEmpty,
        ParseError::InvalidLength {
            min, max, provide, ..
        } => SnmpQueryError::SnmpCommunityInvalidLength { min, max, provide },
        ParseError::Common { message } => SnmpQueryError::Other(message),
        _ => SnmpQueryError::Other("Can't parse community string".to_string()),
    })
}

fn parse_profile(profile: Option<String>) -> Result<Option<SnmpProfile>, SnmpQueryError> {
    profile
        .map(|p| p.parse::<SnmpProfile>())
        .transpose()
        .map_err(|e| SnmpQueryError::InvalidSnmpProfile { message: e })
}

fn resolve_oid(
    raw: &str,
    profile: Option<&SnmpProfile>,
    pos: usize,
) -> Result<SnmpOid, SnmpQueryError> {
    let raw = raw.trim().to_lowercase();

    if let Ok(oid) = SnmpOid::parse(&raw) {
        return Ok(oid);
    }

    let profile = profile.ok_or(SnmpQueryError::SnmpProfileMustBeProvided {
        message: "SNMP profile is required for auto search oid by name".to_string(),
    })?;

    let meta = profile
        .get_metadata_by_name_or_alias(&raw)
        .ok_or(SnmpQueryError::UnknownAlias {
            pos,
            alias: raw.clone(),
        })?;

    SnmpOid::parse(meta.oid).map_err(|_| SnmpQueryError::InvalidSnmpOid {
        pos,
        oid: meta.oid.to_string(),
    })
}

/// Кодирует сырое значение SET в `SnmpValue`:
/// 1) явный `value_type` → 2) builder из реестра → 3) ошибка.
fn encode_set_value(
    oid: &SnmpOid,
    value: &str,
    value_type: Option<String>,
    profile: Option<&SnmpProfile>,
) -> Result<SnmpValue, SnmpQueryError> {
    if let Some(ty) = value_type {
        let ty: SnmpValueType = ty.parse().map_err(SnmpQueryError::Other)?;
        return encode_by_type(ty, value).map_err(|e| SnmpQueryError::Other(e.to_string()));
    }

    if let Some(builder) = profile
        .and_then(|p| p.get_metadata_by_oid(oid))
        .and_then(|m| m.builder)
    {
        return builder(value).map_err(|e| SnmpQueryError::Other(e.to_string()));
    }

    Err(SnmpQueryError::Other(format!("no builder and no value_type for oid {oid}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(oid: &str) -> RawSnmpOidItem {
        RawSnmpOidItem {
            name: None,
            oid: oid.to_string(),
        }
    }

    #[test]
    fn resolves_numeric_oids_without_profile() {
        let q = SnmpGetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            "public".to_string(),
            None,
            vec![raw("1.3.6.1.4.1.1618.3.7.2.11.2")],
        )
        .unwrap();

        assert_eq!(q.host, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(q.oids.len(), 1);
        assert!(q.profile.is_none());
    }

    #[test]
    fn rejects_invalid_ip() {
        let err = SnmpGetQuery::from_raw(
            "not-an-ip".to_string(),
            161,
            "public".to_string(),
            None,
            vec![],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::InvalidIpAddress { .. }));
    }

    #[test]
    fn rejects_empty_community() {
        let err = SnmpGetQuery::from_raw("127.0.0.1".to_string(), 161, String::new(), None, vec![])
            .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpCommunityIsEmpty));
    }

    #[test]
    fn rejects_alias_without_profile() {
        let err = SnmpGetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            "public".to_string(),
            None,
            vec![raw("some_alias")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpProfileMustBeProvided { .. }));
    }

    fn raw_set(oid: &str, value: &str) -> RawSnmpSetItem {
        RawSnmpSetItem {
            name: None,
            oid: oid.to_string(),
            value: value.to_string(),
            value_type: None,
        }
    }

    #[test]
    fn snmp_set_numeric_oid_with_explicit_type() {
        let q = SnmpSetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![RawSnmpSetItem {
                name: None,
                oid: "1.3.6.1.4.1.999.1.0".to_string(),
                value: "0x01".to_string(),
                value_type: Some("octet_string".to_string()),
            }],
        )
        .unwrap();

        assert_eq!(q.sets.len(), 1);
        assert!(matches!(
            q.sets[0].value,
            SnmpValue::OctetString(ref b) if b == &vec![0x01]
        ));
    }

    #[test]
    fn snmp_set_alias_with_profile_uses_builder() {
        let q = SnmpSetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            Some("swarco".to_string()),
            vec![raw_set("set_stage", "3")],
        )
        .unwrap();

        assert_eq!(q.sets.len(), 1);
        assert!(matches!(q.sets[0].value, SnmpValue::Gauge32(3)));
    }

    #[test]
    fn snmp_set_rejects_alias_without_profile() {
        let err = SnmpSetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![raw_set("stage", "3")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpProfileMustBeProvided { .. }));
    }

    #[test]
    fn snmp_set_rejects_without_builder_or_type() {
        let err = SnmpSetQuery::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![raw_set("1.3.6.1.4.1.999.1.0", "3")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::Other(_)));
    }
}
