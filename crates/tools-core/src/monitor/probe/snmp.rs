use std::net::IpAddr;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::time::Duration;

use crate::{
    monitor::probe::error::{ProbeBuildError, SnmpQueryError},
    polling::{AttemptConfig, AttemptError, Pollable},
    snmp::{
        ParseError, SnmpClient, SnmpClientConfig, SnmpError, SnmpGetSample,
        business_value::BusinessValue,
        community::Community,
        oid::SnmpOid,
        profiles::SnmpProfile,
        resolve::{Resolver, SnmpGetItem, SnmpSetItem, encode_set_value},
        value::SnmpValue,
    },
};

const CLIENT_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSnmpSetItem {
    pub name: Option<String>,
    pub oid: String,
    pub value: String,
    #[serde(alias = "syntax")]
    pub value_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnmpGetSpec {
    pub profile: Option<SnmpProfile>,
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub oids: Vec<SnmpGetItem>,
}

#[derive(Debug, Clone)]
pub struct SnmpSetSpec {
    pub profile: Option<SnmpProfile>,
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub community_r: Option<Community>,
    pub sets: Vec<SnmpSetItem>,
}

pub struct SnmpGetProbe {
    client: SnmpClient,
    items: Vec<SnmpGetItem>,
}

impl SnmpGetProbe {
    pub fn new(client: SnmpClient, items: Vec<SnmpGetItem>) -> Self {
        Self { client, items }
    }
}

#[async_trait]
impl Pollable for SnmpGetProbe {
    type Output = Vec<SnmpGetSample>;

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let oids: Vec<SnmpOid> = self.items.iter().map(|i| i.oid.clone()).collect();

        let samples = self
            .client
            .get_many(&oids)
            .await
            .map_err(classify_snmp_error)?
            .into_iter()
            .zip(&self.items)
            .map(|(vb, item)| {
                let parsed_value = item.parser.map(|parser| match parser(&vb.value) {
                    Ok(val) => val,
                    Err(e) => {
                        tracing::error!(target: "snmp_reader", value = ?&vb.value, "{e}");
                        BusinessValue::Text("parse error".to_string())
                    }
                });

                SnmpGetSample {
                    oid_name: item.name.clone(),
                    oid: vb.oid,
                    raw_value: vb.value,
                    value: parsed_value,
                }
            })
            .collect();

        Ok(samples)
    }
}

pub struct SnmpSetProbe {
    client: SnmpClient,
    items: Vec<SnmpSetItem>,
}

impl SnmpSetProbe {
    pub fn new(client: SnmpClient, items: Vec<SnmpSetItem>) -> Self {
        Self { client, items }
    }
}

#[async_trait]
impl Pollable for SnmpSetProbe {
    type Output = ();

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let sets: Vec<(SnmpOid, SnmpValue)> = self
            .items
            .iter()
            .map(|i| (i.oid.clone(), i.value.clone()))
            .collect();

        self.client
            .set_many(&sets)
            .await
            .map_err(classify_snmp_error)?;

        Ok(())
    }
}

fn classify_snmp_error(e: SnmpError) -> AttemptError {
    match e {
        SnmpError::Network { .. } | SnmpError::Timeout { .. } => {
            AttemptError::Transient(e.to_string())
        }
        _ => AttemptError::Fatal(e.to_string()),
    }
}

impl SnmpGetSpec {
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
                Ok(SnmpGetItem {
                    name: raw.name,
                    oid,
                    parser: None,
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

    pub async fn build(self, attempt: AttemptConfig) -> Result<SnmpGetProbe, ProbeBuildError> {
        let client = connect(self.host, self.port, self.community, &attempt).await?;

        let resolver = Resolver::new(client.clone(), self.profile);
        let resolved = resolver
            .resolve_get(self.oids)
            .await
            .map_err(|e| ProbeBuildError::Other(e.to_string()))?;

        Ok(SnmpGetProbe::new(client, resolved))
    }
}

impl SnmpSetSpec {
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
                let value =
                    encode_set_value(&oid, &raw.value, raw.value_type.as_deref(), profile.as_ref())
                        .map_err(|e| SnmpQueryError::Other(e.to_string()))?;
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

    pub async fn build(self, attempt: AttemptConfig) -> Result<SnmpSetProbe, ProbeBuildError> {
        let client = connect(self.host, self.port, self.community, &attempt).await?;

        let scn_client = match self.community_r {
            Some(c) => connect(self.host, self.port, c, &attempt).await?,
            None => client.clone(),
        };

        let resolver = Resolver::new(scn_client, self.profile);
        let resolved = resolver
            .resolve_set(self.sets)
            .await
            .map_err(|e| ProbeBuildError::Other(e.to_string()))?;

        Ok(SnmpSetProbe::new(client, resolved))
    }
}

async fn connect(
    host: IpAddr,
    port: u16,
    community: Community,
    attempt: &AttemptConfig,
) -> Result<SnmpClient, ProbeBuildError> {
    let config = SnmpClientConfig {
        target: host,
        port,
        community,
        timeout: attempt.timeout().saturating_add(CLIENT_TIMEOUT_MARGIN),
        retries: 0,
        retry_delay: attempt.retry_delay(),
    };

    SnmpClient::new(config)
        .await
        .map_err(|_| ProbeBuildError::SnmpClientCreate)
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
    match profile {
        Some(p) => p.resolve_oid(raw).map_err(|e| match e {
            ParseError::UnknownAlias { alias } => SnmpQueryError::UnknownAlias { pos, alias },
            e => SnmpQueryError::Other(e.to_string()),
        }),
        None => SnmpOid::parse(raw.trim()).map_err(|_| SnmpQueryError::SnmpProfileMustBeProvided {
            message: "SNMP profile is required for auto search oid by name".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snmp::value::SnmpValue;

    fn raw(oid: &str) -> RawSnmpOidItem {
        RawSnmpOidItem {
            name: None,
            oid: oid.to_string(),
        }
    }

    #[test]
    fn resolves_numeric_oids_without_profile() {
        let q = SnmpGetSpec::from_raw(
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
        let err = SnmpGetSpec::from_raw(
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
        let err = SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, String::new(), None, vec![])
            .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpCommunityIsEmpty));
    }

    #[test]
    fn rejects_alias_without_profile() {
        let err = SnmpGetSpec::from_raw(
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
        let q = SnmpSetSpec::from_raw(
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
        let q = SnmpSetSpec::from_raw(
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
        let err = SnmpSetSpec::from_raw(
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
        let err = SnmpSetSpec::from_raw(
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
