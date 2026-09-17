use std::net::IpAddr;

use tokio::time::Duration;

use async_trait::async_trait;

use crate::{
    monitor::adapter::{Query, SnmpGetQuery, SnmpSetQuery},
    polling::{AttemptConfig, AttemptError, Pollable},
    snmp::{
        SnmpClient, SnmpClientConfig, SnmpGetQueryItem, SnmpGetResponse, SnmpSetResponse,
        adapters::{SnmpReader, SnmpWriter},
        community::Community,
        resolve::Resolver,
    },
};

use super::error::AdapterBuildError;

const CLIENT_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

pub enum Adapter {
    SnmpGet(SnmpReader),
    SnmpSet(SnmpWriter),
    // HttpRead(HttpReader), // будущий
}

#[derive(Clone, Debug)]
pub enum AdapterOutput {
    SnmpGet(SnmpGetResponse),
    SnmpSet(SnmpSetResponse),
    // HttpRead(HttpReadResponse),
}

#[async_trait]
impl Pollable for Adapter {
    type Output = AdapterOutput;

    async fn poll(&self) -> Result<AdapterOutput, AttemptError> {
        match self {
            Self::SnmpGet(a) => a.poll().await.map(AdapterOutput::SnmpGet),
            Self::SnmpSet(a) => a.poll().await.map(AdapterOutput::SnmpSet),
        }
    }
}

impl Adapter {
    pub async fn build(query: Query, attempt: AttemptConfig) -> Result<Self, AdapterBuildError> {
        match query {
            Query::SnmpGet(q) => Self::build_snmp_get(q, attempt).await,
            Query::SnmpSet(q) => Self::build_snmp_set(q, attempt).await,
        }
    }

    async fn build_snmp_get(
        q: SnmpGetQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, AdapterBuildError> {
        let client = Self::connect(q.host, q.port, q.community, &attempt).await?;

        let items = q
            .oids
            .into_iter()
            .map(|item| SnmpGetQueryItem {
                name: item.name,
                oid: item.oid,
                business_value_parser: None,
            })
            .collect();

        let resolver = Resolver::new(client.clone(), q.profile);
        let resolved = resolver
            .resolve_get(items)
            .await
            .map_err(|e| AdapterBuildError::Other(e.to_string()))?;

        Ok(Self::SnmpGet(SnmpReader::new(client, resolved)))
    }

    async fn build_snmp_set(
        q: SnmpSetQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, AdapterBuildError> {
        let client = Self::connect(q.host, q.port, q.community, &attempt).await?;

        // Клиент для чтения SCN: по умолчанию write-клиент (common-кейс — общий
        // community); отдельный read-клиент нужен только при write-only community.
        let scn_client = match q.community_r {
            Some(c) => Self::connect(q.host, q.port, c, &attempt).await?,
            None => client.clone(),
        };

        let resolver = Resolver::new(scn_client, q.profile);
        let resolved = resolver
            .resolve_set(q.sets)
            .await
            .map_err(|e| AdapterBuildError::Other(e.to_string()))?;

        Ok(Self::SnmpSet(SnmpWriter::new(client, resolved)))
    }

    async fn connect(
        host: IpAddr,
        port: u16,
        community: Community,
        attempt: &AttemptConfig,
    ) -> Result<SnmpClient, AdapterBuildError> {
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
            .map_err(|_| AdapterBuildError::SnmpClientCreate)
    }
}
