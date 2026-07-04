use std::net::IpAddr;

use async_trait::async_trait;

use crate::models::{PollType, ProviderConfig};

#[async_trait]
pub trait Pollable: Send + Sync {
    async fn fetch(&self) -> Result<String, String>;
    fn username(&self) -> String;
    fn target(&self) -> IpAddr;
    fn whoami(&self) -> PollType;
    fn dump(&self) -> ProviderConfig;
}

#[async_trait]
pub trait TracerouteProvider: Send + Sync {
    async fn traceroute(&self) -> anyhow::Result<String>;
    fn max_hops(&self) -> u8;
    fn queries_per_hop(&self) -> u8;
    fn probe_timeout_millis(&self) -> u64;
    fn dump(&self) -> ProviderConfig;
}
