use std::net::IpAddr;

use crate::snmp::{
    community::Community,
    oid::SnmpOid,
    profiles::SnmpProfile,
    ParseError,
};

use super::error::QueryError;

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

/// Валидированный SNMP GET запрос: доменные типы вместо сырых строк.
#[derive(Debug, Clone)]
pub struct QuerySnmpGet {
    pub profile: Option<SnmpProfile>,
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub oids: Vec<SnmpOidItem>,
}

/// Запрос задачи. Валидация сырых значений происходит при построении
/// (`QuerySnmpGet::from_raw`), поэтому здесь уже доменные типы.
#[derive(Clone, Debug)]
pub enum UseCaseQuery {
    SnmpGet(QuerySnmpGet),
    // SnmpSet(QuerySnmpSet),
    // HttpRead(QueryHttpRead),
}

impl UseCaseQuery {
    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(q) => format!("{}:{}", q.host, q.port),
        }
    }
}

impl QuerySnmpGet {
    /// Собрать валидированный SNMP GET запрос из сырых значений конфига.
    pub fn from_raw(
        host: String,
        port: u16,
        community: String,
        profile: Option<String>,
        oids: Vec<RawSnmpOidItem>,
    ) -> Result<Self, QueryError> {
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
            .collect::<Result<Vec<_>, QueryError>>()?;

        Ok(Self {
            profile,
            host,
            port,
            community,
            oids,
        })
    }
}

fn parse_ip(ip: &str) -> Result<IpAddr, QueryError> {
    ip.parse::<IpAddr>()
        .map_err(|_| QueryError::InvalidIpAddress { ip: ip.to_string() })
}

fn parse_community(community: &str) -> Result<Community, QueryError> {
    Community::parse(community.to_string()).map_err(|e| match e {
        ParseError::CantBeEmpty { .. } => QueryError::SnmpCommunityIsEmpty,
        ParseError::InvalidLength {
            min, max, provide, ..
        } => QueryError::SnmpCommunityInvalidLength { min, max, provide },
        ParseError::Common { message } => QueryError::Other(message),
        _ => QueryError::Other("Can't parse community string".to_string()),
    })
}

fn parse_profile(profile: Option<String>) -> Result<Option<SnmpProfile>, QueryError> {
    profile
        .map(|p| p.parse::<SnmpProfile>())
        .transpose()
        .map_err(|e| QueryError::InvalidSnmpProfile { message: e })
}

fn resolve_oid(
    raw: &str,
    profile: Option<&SnmpProfile>,
    pos: usize,
) -> Result<SnmpOid, QueryError> {
    let raw = raw.trim().to_lowercase();

    if let Ok(oid) = SnmpOid::parse(&raw) {
        return Ok(oid);
    }

    let profile = profile.ok_or(QueryError::SnmpProfileMustBeProvided {
        message: "SNMP profile is required for auto search oid by name".to_string(),
    })?;

    let meta = profile
        .get_metadata_by_name_or_alias(&raw)
        .ok_or(QueryError::UnknownAlias {
            pos,
            alias: raw.clone(),
        })?;

    SnmpOid::parse(meta.oid).map_err(|_| QueryError::InvalidSnmpOid {
        pos,
        oid: meta.oid.to_string(),
    })
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
        let q = QuerySnmpGet::from_raw(
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
        let err = QuerySnmpGet::from_raw(
            "not-an-ip".to_string(),
            161,
            "public".to_string(),
            None,
            vec![],
        )
        .unwrap_err();

        assert!(matches!(err, QueryError::InvalidIpAddress { .. }));
    }

    #[test]
    fn rejects_empty_community() {
        let err = QuerySnmpGet::from_raw(
            "127.0.0.1".to_string(),
            161,
            String::new(),
            None,
            vec![],
        )
        .unwrap_err();

        assert!(matches!(err, QueryError::SnmpCommunityIsEmpty));
    }

    #[test]
    fn rejects_alias_without_profile() {
        let err = QuerySnmpGet::from_raw(
            "127.0.0.1".to_string(),
            161,
            "public".to_string(),
            None,
            vec![raw("some_alias")],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            QueryError::SnmpProfileMustBeProvided { .. }
        ));
    }
}
