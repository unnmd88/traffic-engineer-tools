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

        let oids = q
            .oids
            .into_iter()
            .map(|item| SnmpGetQueryItem {
                name: item.name,
                oid: item.oid,
                business_value_parser: None,
            })
            .collect();

        let reader = SnmpReader::new(client, oids, q.profile)
            .await
            .map_err(|e| AdapterBuildError::Other(e.to_string()))?;

        Ok(Self::SnmpGet(reader))
    }

    async fn build_snmp_set(
        q: SnmpSetQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, AdapterBuildError> {
        let client = Self::connect(q.host, q.port, q.community, &attempt).await?;

        let writer = SnmpWriter::new(client, q.sets, q.profile)
            .await
            .map_err(|e| AdapterBuildError::Other(e.to_string()))?;

        Ok(Self::SnmpSet(writer))
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
