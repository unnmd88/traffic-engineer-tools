use std::net::{IpAddr, SocketAddr};

use async_snmp::{Auth, Client, Retry};
use tokio::time::Duration;

use crate::snmp::{
    SnmpError,
    community::Community, oid::SnmpOid, value::SnmpValue, varbind::SnmpVarbind,
};

#[derive(Debug, Clone)]
pub struct SnmpClientConfig {
    pub target: IpAddr,
    pub port: u16,
    pub community: Community,
    pub timeout: Duration,
    pub retries: u32,
    pub retry_delay: Duration,
}

impl SnmpClientConfig {
    pub async fn connect(&self) -> Result<Client, SnmpError> {
        Client::builder((self.target.to_string(), self.port), Auth::v2c(self.community.clone()))
            .timeout(self.timeout)
            .retry(if self.retries > 0 {
                Retry::fixed(self.retries, self.retry_delay)
            } else {
                Retry::none()
            })
            .connect()
            .await
            .map_err(|e| {
                tracing::warn!(target: "create snmp client", "{e}");
                SnmpError::Internal(format!("create snmp client: {e}"))
            })
    }
}

#[derive(Clone)]
pub struct SnmpClient {
    client: Client,
    config: SnmpClientConfig,
}

impl SnmpClient {
    pub async fn new(config: SnmpClientConfig) -> Result<Self, SnmpError> {
        let client = config.connect().await?;
        Ok(Self { client, config })
    }

    pub fn config(&self) -> &SnmpClientConfig {
        &self.config
    }

    pub async fn get(&self, oid: &SnmpOid) -> Result<SnmpVarbind, SnmpError> {
        self.get_many(&[oid.clone()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| SnmpError::Internal("Ошибка выполнения запроса".to_string()))
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
        async_snmp::Error::Network { target, source } => {
            tracing::warn!(target: "snmp network error", "{source}");
            SnmpError::Network {
                target,
                reason: source.to_string(),
            }
        }
        async_snmp::Error::Timeout { target, retries, .. } => SnmpError::Timeout { target, retries },
        async_snmp::Error::Auth { target } => SnmpError::Auth { target },
        async_snmp::Error::Snmp {
            target,
            status,
            index,
            oid,
        } => SnmpError::Protocol {
            target,
            status: status.to_string(),
            index,
            oid: oid.map(|o| o.to_string()),
        },
        async_snmp::Error::InvalidOid(msg) => SnmpError::InvalidOid(msg.to_string()),
        _ => SnmpError::Internal(e.to_string()),
    }
}
