use tokio::time::Duration;

use async_trait::async_trait;

use crate::{
    monitor::adapter::{Query, SnmpGetQuery},
    polling::{AttemptConfig, AttemptError, Pollable},
    snmp::{SnmpClient, SnmpClientConfig, SnmpGetQueryItem, SnmpGetResponse, adapters::SnmpReader},
};

use super::error::AdapterBuildError;

const CLIENT_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

pub enum Adapter {
    SnmpGet(SnmpReader),
    // SnmpSet(SnmpWriter),  // будущий
    // HttpRead(HttpReader), // будущий
}

#[derive(Clone, Debug)]
pub enum AdapterOutput {
    SnmpGet(SnmpGetResponse),
    // SnmpSet(SnmpSetResponse),
    // HttpRead(HttpReadResponse),
}

#[async_trait]
impl Pollable for Adapter {
    type Output = AdapterOutput;

    async fn poll(&self) -> Result<AdapterOutput, AttemptError> {
        match self {
            Self::SnmpGet(a) => a.poll().await.map(AdapterOutput::SnmpGet),
        }
    }
}

impl Adapter {
    pub async fn build(query: Query, attempt: AttemptConfig) -> Result<Self, AdapterBuildError> {
        match query {
            Query::SnmpGet(q) => Self::build_snmp_get(q, attempt).await,
        }
    }

    async fn build_snmp_get(
        q: SnmpGetQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, AdapterBuildError> {
        let client_config = SnmpClientConfig {
            target: q.host,
            port: q.port,
            community: q.community,
            // добавить CLIENT_TIMEOUT_MARGIN, чтобы внутренний таймаут не наступил раньше чем в async poll.
            timeout: attempt.timeout().saturating_add(CLIENT_TIMEOUT_MARGIN),
            // Ретраями управляет async poll
            retries: 0,
            retry_delay: attempt.retry_delay(),
        };

        let client = SnmpClient::new(client_config)
            .await
            .map_err(|_| AdapterBuildError::SnmpClientCreate)?;

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
}
