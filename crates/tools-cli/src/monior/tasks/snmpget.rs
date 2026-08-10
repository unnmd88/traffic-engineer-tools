use std::net::IpAddr;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tools_core::snmp::{
    SnmpQueryItem,
    primitives::{Community, SnmpOid},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct SnmpOidItem {
    name: Option<String>,
    oid: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TaskSnmpGetDto {
    pub name: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub timeout_ms: u64,
    pub retries: u32,
    pub retry_delay_ms: u64,
    pub oids: Vec<SnmpOidItem>,
}

#[derive(Debug, Deserialize)]
pub struct TaskSnmpGet {
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub timeout_ms: u64,
    pub retries: u32,
    pub retry_delay_ms: u64,
    pub oids: Vec<SnmpQueryItem>,
}

impl TryFrom<TaskSnmpGetDto> for TaskSnmpGet {
    type Error = anyhow::Error;

    fn try_from(value: TaskSnmpGetDto) -> Result<Self, Self::Error> {
        let host = value
            .host
            .parse::<IpAddr>()
            .context(format!("Invalid IP-address: {}", value.host))?;

        let oids = value
            .oids
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                SnmpOid::parse(&item.oid)
                    //.with_context(|| format!("Некорректный OID(индекс={i}): {}", item.oid))
                    .map(|oid| SnmpQueryItem {
                        name: item.name,
                        oid,
                    })
                    .map_err(|e| anyhow::anyhow!("Oid pos: {}: {}", i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            host,
            port: value.port,
            community: Community::parse(value.community)?,
            timeout_ms: value.timeout_ms,
            retries: value.retries,
            retry_delay_ms: value.retry_delay_ms,
            oids,
        })
    }
}
