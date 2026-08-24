use anyhow::Result;
use std::net::{IpAddr, SocketAddr};
use tokio::time::Duration;

use async_snmp::{Auth, Client, Oid, Retry, VarBind};
use serde::Serialize;

use crate::{
    SnmpError,
    snmp::{community::Community, oid::SnmpOid, value::SnmpValue, varbind::SnmpVarbind},
};

#[derive(Debug, Clone)]
pub struct SnmpReadClientConfig {
    pub target: IpAddr,
    pub port: u16,
    pub community: Community,
    pub timeout: Duration,
    pub retries: u32,
    pub retry_delay: Duration,
}

async fn create_inner_client(
    target: IpAddr,
    port: u16,
    community: Community,
    timeout: Duration,
    retries: u32,
    retry_delay: Duration,
) -> Result<Client, SnmpError> {
    let client = Client::builder((target.to_string(), port), Auth::v2c(community))
        .timeout(timeout)
        .retry(if retries > 0 {
            Retry::fixed(retries, retry_delay)
        } else {
            Retry::none()
        })
        .connect()
        .await
        .map_err(|e| {
            tracing::warn!(target: "Create snmp client", "{}", e);
            SnmpError::ConnectionFailed { target, port }
        })?;
    Ok(client)
}

#[derive(Clone)]
pub struct SnmpReadClient {
    client: Client,
    config: SnmpReadClientConfig,
}

#[derive(Clone)]
pub struct SnmpWriteClient {
    // !TODO
    client: Client,
}

#[derive(Clone)]
pub struct SnmpReadWriteClient {
    // !TODO
    read_client: SnmpReadClient,
    write_client: SnmpWriteClient,
}

impl SnmpReadClient {
    pub async fn new(config: SnmpReadClientConfig) -> Result<Self, SnmpError> {
        let client = create_inner_client(
            config.target,
            config.port,
            config.community.clone(),
            config.timeout,
            config.retries,
            config.retry_delay,
        )
        .await?;
        Ok(Self { client, config })
    }

    pub fn config(&self) -> &SnmpReadClientConfig {
        &self.config
    }

    pub async fn get(&self, oid: &SnmpOid) -> Result<SnmpVarbind, SnmpError> {
        self.get_many(&[oid.clone()])
            .await?
            .into_iter()
            .next()
            .ok_or(SnmpError::Internal("Ошибка выполнения запроса".to_string()))
    }

    pub async fn get_many(&self, oids: &[SnmpOid]) -> Result<Vec<SnmpVarbind>, SnmpError> {
        let lib_oids: Vec<async_snmp::Oid> = oids.iter().map(|oid| oid.inner().clone()).collect();

        let varbinds = self
            .client
            .get_many(&lib_oids)
            .await
            .map_err(|e| map_snmp_error(*e))?;

        Ok(varbinds
            .into_iter()
            .map(|vb| SnmpVarbind {
                oid: SnmpOid::new(vb.oid),
                value: SnmpValue::from(&vb.value),
            })
            .collect())
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.client.peer_addr()
    }
}

fn map_snmp_error(e: async_snmp::Error) -> SnmpError {
    match e {
        async_snmp::Error::Network { target, source } => SnmpError::ConnectionFailed {
            target: target.ip(),
            port: target.port(),
        },
        async_snmp::Error::Timeout {
            target, retries, ..
        } => SnmpError::RequestTimeOut { target, retries },
        _ => SnmpError::Internal(e.to_string()),
    }
}
