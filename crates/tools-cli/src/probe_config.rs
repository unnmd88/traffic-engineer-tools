use std::{fs, net::IpAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use tools_core::snmp::{SnmpQueryItem, primitives::Community};

fn default_port() -> u16 {
    161
}

fn default_timeout_ms() -> u64 {
    1000
}

fn default_retries() -> u64 {
    0
}

fn default_retry_delay_ms() -> u64 {
    0
}

fn default_community() -> Community {
    Community::new("public".to_string())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
pub enum Probe {
    SnmpGet(ProbeSnmpGet),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProbeSnmpGet {
    pub host: IpAddr,
    pub port: u16,
    pub community: Community,
    pub timeout_ms: u64,
    pub retries: u32,
    pub retry_delay_ms: u64,
    pub oids: Vec<SnmpQueryItem>,
}

impl Default for ProbeSnmpGet {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".parse().unwrap(),
            port: 161,
            community: Community("public".to_string()),
            timeout_ms: 1000,
            retries: 3,
            retry_delay_ms: 200,
            oids: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProbeConfig {
    pub probes: Vec<Probe>,
}
impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            probes: vec![Probe::SnmpGet(ProbeSnmpGet::default())],
        }
    }
}

impl ProbeConfig {
    pub fn from_yaml(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }

    pub fn generate_default_config() -> anyhow::Result<()> {
        let config = ProbeConfig::default();
        let yaml = serde_yaml::to_string(&config)?;
        fs::write("probe.yaml", yaml)?;
        println!("✅ Generated probe.yaml");
        Ok(())
    }
}
