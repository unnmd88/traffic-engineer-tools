use std::net::IpAddr;

use async_trait::async_trait;

use crate::{
    error::{BuildMonitorError, ParseError, PollError},
    monitor::application::config::{QuerySnmpGet, SnmpOidItem, UseCaseQuery},
    polling::{AttemptConfig, Pollable},
    snmp::{
        SnmpGetQueryItem, SnmpGetResponse, SnmpReadClient, SnmpReadClientConfig,
        adapters::SnmpReader,
        community::Community,
        oid::SnmpOid,
        profiles::SnmpProfile,
    },
};

pub enum UseCase {
    SnmpGet(SnmpReader),
    // SnmpSet(SnmpWriter),  // будущий
    // HttpRead(HttpReader), // будущий
}

#[derive(Clone, Debug)]
pub enum UseCaseOutput {
    SnmpGet(SnmpGetResponse),
    // SnmpSet(SnmpSetResponse),
    // HttpRead(HttpReadResponse),
}

#[async_trait]
impl Pollable for UseCase {
    type Output = UseCaseOutput;

    async fn poll(&self) -> Result<UseCaseOutput, PollError> {
        match self {
            Self::SnmpGet(a) => a.poll().await.map(UseCaseOutput::SnmpGet),
        }
    }
}

impl UseCase {
    pub async fn build(
        query: UseCaseQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, BuildMonitorError> {
        match query {
            UseCaseQuery::SnmpGet(q) => Self::build_snmp_get(q, attempt).await,
        }
    }

    async fn build_snmp_get(
        q: QuerySnmpGet,
        attempt: AttemptConfig,
    ) -> Result<Self, BuildMonitorError> {
        let target = parse_ip(&q.host)?;
        let community = parse_community(&q.community)?;
        let profile = parse_profile(q.profile)?;

        let client_config = SnmpReadClientConfig {
            target,
            port: q.port,
            community,
            timeout: attempt.timeout,
            retries: attempt.retries as u32,
            retry_delay: attempt.retry_delay,
        };

        let client = SnmpReadClient::new(client_config)
            .await
            .map_err(|_| BuildMonitorError::SnmpClientCreate)?;

        let oids = sanitize_oids(&q.oids, profile.as_ref())?;
        let reader = SnmpReader::new(client, oids, profile)
            .await
            .map_err(|e| BuildMonitorError::Other(e.to_string()))?;

        Ok(Self::SnmpGet(reader))
    }
}

fn parse_ip(ip: &str) -> Result<IpAddr, BuildMonitorError> {
    ip.parse::<IpAddr>()
        .map_err(|_| BuildMonitorError::InvalidIpAddress { ip: ip.to_string() })
}

fn parse_community(community: &str) -> Result<Community, BuildMonitorError> {
    Community::parse(community.to_string()).map_err(|e| match e {
        ParseError::CantBeEmpty { .. } => BuildMonitorError::SnmpCommunityIsEmpty,
        ParseError::InvalidLength {
            min, max, provide, ..
        } => BuildMonitorError::SnmpCommunityInvalidLength { min, max, provide },
        ParseError::Common { message } => BuildMonitorError::Other(message),
        _ => BuildMonitorError::Other("Can't parse community string".to_string()),
    })
}

fn parse_profile(profile: Option<String>) -> Result<Option<SnmpProfile>, BuildMonitorError> {
    profile
        .map(|p| p.parse::<SnmpProfile>())
        .transpose()
        .map_err(|e| BuildMonitorError::InvalidSnmpProfile { message: e })
}

fn sanitize_oids(
    oids: &[SnmpOidItem],
    profile: Option<&SnmpProfile>,
) -> Result<Vec<SnmpGetQueryItem>, BuildMonitorError> {
    oids.iter()
        .enumerate()
        .map(|(pos, item)| {
            let oid = resolve_oid(&item.oid, profile, pos)?;
            Ok(SnmpGetQueryItem {
                name: item.name.clone(),
                oid,
                business_value_parser: None,
            })
        })
        .collect()
}

fn resolve_oid(
    raw: &str,
    profile: Option<&SnmpProfile>,
    pos: usize,
) -> Result<SnmpOid, BuildMonitorError> {
    let raw = raw.trim().to_lowercase();

    if let Ok(oid) = SnmpOid::parse(&raw) {
        return Ok(oid);
    }

    let profile = profile.ok_or(BuildMonitorError::SnmpProfileMustBeProvided {
        message: "SNMP profile is required for auto search oid by name".to_string(),
    })?;

    let meta = profile
        .get_metadata_by_name_or_alias(&raw)
        .ok_or(BuildMonitorError::UnknownAlias {
            pos,
            alias: raw.clone(),
        })?;

    SnmpOid::parse(meta.oid)
        .map_err(|_| BuildMonitorError::InvalidSnmpOid { pos, oid: meta.oid.to_string() })
}
