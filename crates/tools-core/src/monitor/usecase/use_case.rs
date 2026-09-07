use tokio::time::Duration;

use async_trait::async_trait;

use crate::{
    monitor::task::{QuerySnmpGet, UseCaseQuery},
    polling::{AttemptConfig, AttemptError, Pollable},
    snmp::{
        SnmpClient, SnmpClientConfig, SnmpGetQueryItem, SnmpGetResponse, adapters::SnmpReader,
    },
};

use super::error::UseCaseBuildError;

const CLIENT_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

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

    async fn poll(&self) -> Result<UseCaseOutput, AttemptError> {
        match self {
            Self::SnmpGet(a) => a.poll().await.map(UseCaseOutput::SnmpGet),
        }
    }
}

impl UseCase {
    pub async fn build(
        query: UseCaseQuery,
        attempt: AttemptConfig,
    ) -> Result<Self, UseCaseBuildError> {
        match query {
            UseCaseQuery::SnmpGet(q) => Self::build_snmp_get(q, attempt).await,
        }
    }

    async fn build_snmp_get(
        q: QuerySnmpGet,
        attempt: AttemptConfig,
    ) -> Result<Self, UseCaseBuildError> {
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
            .map_err(|_| UseCaseBuildError::SnmpClientCreate)?;

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
            .map_err(|e| UseCaseBuildError::Other(e.to_string()))?;

        Ok(Self::SnmpGet(reader))
    }
}
