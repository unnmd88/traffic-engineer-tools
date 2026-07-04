use std::{fmt, net::IpAddr};

use clap::builder::Str;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    Independent,
    Synchronized,
}

impl fmt::Display for Strategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Strategy::Independent => "independent",
            Strategy::Synchronized => "synchronized",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollType {
    Ping,
    Snmp,
    Traceroute,
}

impl fmt::Display for PollType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PollType::Ping => write!(f, "PING"),
            PollType::Snmp => write!(f, "SNMP"),
            PollType::Traceroute => write!(f, "TRACEROUTE"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TracerouteEngine {
    Trippy,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AboutApp {
    pub name: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "sid")]
    pub session_id: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub username: String,
    pub test_type: PollType,
    pub target: IpAddr,
    pub start: String,
    pub end: String,
    pub success: bool,
    pub attempts: u8,
    pub latency_ms: f64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    PollResult {
        strategy: Strategy,
        step: usize,
        #[serde(flatten)]
        payload: FetchResult,
    },

    Config {
        strategy: Strategy,
        // #[serde(flatten)]
        details: serde_json::Value,
    },

    StartApplication {
        details: AboutApp,
    },

    Error {
        error_type: String,
        message: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndependentPollerConfig {
    #[serde(flatten)]
    pub provider: ProviderConfig,
    pub retries: u8,
    pub retries_interval_ms: u64,
    pub interval_ms: u64,
    pub fallback: Option<ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IndependentConfigDetails {
    pub pollers: Vec<IndependentPollerConfig>,
    pub num_pollers: u8,
}

impl IndependentConfigDetails {
    pub fn new(pollers: Vec<IndependentPollerConfig>) -> Self {
        Self {
            num_pollers: pollers.len() as u8,
            pollers,
        }
    }

    pub fn as_json(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(&self)?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SynchronizedProviderConfig {
    #[serde(flatten)]
    pub provider: ProviderConfig,
    pub fallback: Option<ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SynchronizedConfigDetails {
    pub providers: Vec<SynchronizedProviderConfig>,
    pub interval_ms: u64,
    pub num_providers: u8,
}

impl SynchronizedConfigDetails {
    pub fn new(
        providers: Vec<SynchronizedProviderConfig>,
        interval_ms: u64,
    ) -> Self {
        Self {
            interval_ms,
            num_providers: providers.len() as u8,
            providers,
        }
    }

    pub fn as_json(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(&self)?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ProviderConfig {
    Ping {
        username: String,
        target: IpAddr,
        timeout_ms: u64,
        extra: Option<serde_json::Value>,
    },
    Snmp {
        username: String,
        target: IpAddr,
        timeout_ms: u64,
        port: u16,
        //   community: String,
        oids: Vec<String>,
        extra: Option<serde_json::Value>,
    },
    Traceroute {
        username: String,
        target: IpAddr,
        timeout_ms: u64,
        engine: TracerouteEngine,
        max_hops: u8,
        queries_per_hop: u8,
        extra: Option<serde_json::Value>,
    },
}
