use std::net::IpAddr;
use tokio::task;

use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;
use trippy_core::{Builder, Protocol};

use crate::{
    models::{PollType, ProviderConfig, TracerouteEngine},
    traits::{Pollable, TracerouteProvider},
};

#[derive(Debug, Serialize)]
pub struct TrippyTracertProvider {
    username: String,
    target: IpAddr,
    max_hops: u8,
    queries_per_hop: u8,
}

impl TrippyTracertProvider {
    pub async fn new(
        username: String,
        target: IpAddr,
        max_hops: u8,
        queries_per_hop: u8,
    ) -> Self {
        Self {
            username,
            target,
            max_hops,
            // probe_timeout_seconds,
            queries_per_hop,
        }
    }

    pub fn get_extra(&self) -> Option<serde_json::Value> {
        None
    }

    pub fn whoami(&self) -> PollType {
        PollType::Traceroute
    }

    pub fn username(&self) -> String {
        self.username.clone()
    }

    pub fn dump(&self) -> ProviderConfig {
        ProviderConfig::Traceroute {
            engine: TracerouteEngine::Trippy,
            username: self.username(),
            target: self.target,
            timeout_ms: 0,
            max_hops: self.max_hops,
            queries_per_hop: self.queries_per_hop,
            extra: self.get_extra(),
        }
    }
}

#[async_trait]
impl TracerouteProvider for TrippyTracertProvider {
    async fn traceroute(&self) -> anyhow::Result<String> {
        let target = self.target;
        let max_hops = self.max_hops;
        let queries_per_hop = self.queries_per_hop;

        // Запускаем синхронную трассировку в отдельном потоке
        let result = task::spawn_blocking(move || {
            let tracer = Builder::new(target)
                .protocol(Protocol::Icmp)
                .first_ttl(1)
                .max_ttl(max_hops)
                .max_rounds(Some(queries_per_hop as usize))
                .build()
                .context("Ошибка создания трассировщика")?;

            tracer
                .run()
                .context("Ошибка выполнения трассировки")?;

            let state = tracer.snapshot();

            // Формируем вывод
            let mut output = String::new();
            output.push_str(&format!("\n=== Traceroute to {} ===\n", target));
            output.push_str(&format!(
                "{:<3} {:<20} {:<15} {:<10}\n",
                "TTL", "Address", "Avg RTT (ms)", "Loss%"
            ));
            output.push_str(&"-".repeat(60));
            output.push('\n');

            for hop in state.hops() {
                let ttl = hop.ttl();
                let addr = hop
                    .addrs()
                    .next()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "*".to_string());
                let rtt = hop.avg_ms();
                let rtt_str = if rtt > 0.0 {
                    format!("{:.2}", rtt)
                } else {
                    "---".to_string()
                };
                let loss = hop.loss_pct();

                output.push_str(&format!(
                    "{:<3} {:<20} {:<15} {:<10.1}%\n",
                    ttl, addr, rtt_str, loss
                ));
            }

            Ok::<String, anyhow::Error>(output)
        })
        .await
        .context("Ошибка при выполнении трассировки в потоке")??;

        Ok(result)
    }

    fn max_hops(&self) -> u8 {
        self.max_hops
    }
    fn queries_per_hop(&self) -> u8 {
        self.queries_per_hop
    }
    fn probe_timeout_millis(&self) -> u64 {
        0u64
    }

    fn dump(&self) -> ProviderConfig {
        self.dump()
    }
}

#[async_trait]
impl Pollable for TrippyTracertProvider {
    async fn fetch(&self) -> Result<String, String> {
        self.traceroute()
            .await
            .map_err(|e| e.to_string())
    }
    fn username(&self) -> String {
        self.username.clone()
    }
    fn target(&self) -> IpAddr {
        self.target
    }
    fn whoami(&self) -> PollType {
        PollType::Traceroute
    }
    fn dump(&self) -> ProviderConfig {
        self.dump()
    }
}
