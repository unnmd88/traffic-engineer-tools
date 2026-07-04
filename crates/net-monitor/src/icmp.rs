use async_trait::async_trait;
use ping_async::{IcmpEchoRequestor, IcmpEchoStatus};
use serde::Serialize;
use serde_json;
use std::net::IpAddr;
use std::time::Duration;

use crate::models::{PollType, ProviderConfig};
use crate::traits::Pollable;
use crate::traits::TracerouteProvider;
use trippy_core::{Builder, Protocol};

#[derive(Debug, Serialize)]
pub struct TracertProvider {
    target: IpAddr,
    max_hops: u8,
    // probe_timeout_seconds: u64,
    queries_per_hop: u8,
}

impl TracertProvider {
    pub fn new(
        target: IpAddr,
        max_hops: u8,
        // probe_timeout_seconds: u64,
        queries_per_hop: u8,
    ) -> Result<Self, String> {
        Ok(Self {
            target,
            max_hops,
            // probe_timeout_seconds,
            queries_per_hop,
        })
    }

    pub async fn traceroute(&self) -> String {
        let mut output = String::new();

        let tracer = match Builder::new(self.target)
            .protocol(Protocol::Icmp)
            .first_ttl(1)
            .max_ttl(self.max_hops)
            .max_rounds(Some(self.queries_per_hop as usize))
            .build()
        {
            Ok(t) => t,
            Err(e) => return format!("Ошибка создания трассировщика: {}", e),
        };

        if let Err(e) = tracer.run() {
            return format!("Ошибка выполнения трассировки: {}", e);
        }

        let state = tracer.snapshot();

        output.push_str(&format!("\n=== Traceroute to {} ===\n", self.target));
        output.push_str(&format!(
            "{:<3} {:<20} {:<15} {:<10}\n",
            "TTL", "Address", "Avg RTT (ms)", "Loss%"
        ));
        output.push_str(&"-".repeat(60));
        output.push('\n');

        for hop in state.hops() {
            let ttl = hop.ttl();

            // addrs() возвращает итератор, берём первый адрес
            let addr = hop
                .addrs()
                .next()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "*".to_string());

            // avg_ms() возвращает f64 напрямую
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

        output
    }
}

pub struct IcmpProvider {
    username: String,
    pub target: IpAddr,
    requestor: IcmpEchoRequestor,
    pub timeout_ms: u64,
}

impl IcmpProvider {
    pub fn new(
        username: String,
        target: IpAddr,
        timeout_ms: u64,
    ) -> Result<Self, String> {
        let requestor = IcmpEchoRequestor::new(
            target,
            None,
            None,
            //Some(Duration::from_secs(timeout_seconds)),
            Some(Duration::from_millis(timeout_ms)),
        )
        .map_err(|e| format!("Ошибка создания ping-опроса: {e}"))?;

        Ok(Self {
            username,
            target,
            requestor,
            timeout_ms,
        })
    }

    async fn ping_once(&self) -> Result<String, String> {
        // let reply = self.requestor.send().await.map_err(|e| format!("Ошибка: {}", e))?;

        let reply = match self.requestor.send().await {
            Ok(reply) => reply,
            Err(e) => return Err(format!("Ошибка: {}", e)),
        };

        let result = match reply.status() {
            IcmpEchoStatus::Success => {
                Ok(format!("Успех. RTT: {:?}", reply.round_trip_time()))
            }
            IcmpEchoStatus::TimedOut => Err(format!(
                "Превышен таймаут ответа.: {:?}",
                reply.round_trip_time()
            )),
            IcmpEchoStatus::Unreachable => Err(format!(
                "Хост недоступен. RTT: {:?}",
                reply.round_trip_time()
            )),
            IcmpEchoStatus::Unknown => Err(format!("Ошибка запроса(Unknown)")),
        };

        result
    }

    fn get_extra(&self) -> Option<serde_json::Value> {
        Some(serde_json::Value::Null)

        /*

                self.tracert.as_ref().and_then(|t| {
                    serde_json::to_value(t)
                        .ok()
                        .map(|v| json!({"tracert": v}))
                })
        */
    }

    pub fn dump(&self) -> ProviderConfig {
        ProviderConfig::Ping {
            username: self.username(),
            target: self.target,
            timeout_ms: self.timeout_ms,
            extra: self.get_extra(),
        }
    }

    pub fn name(&self) -> String {
        self.username.clone()
    }
}

#[async_trait]
impl Pollable for IcmpProvider {
    async fn fetch(&self) -> Result<String, String> {
        //self.ping().await
        //self.ping2().await
        self.ping_once().await
    }

    fn dump(&self) -> ProviderConfig {
        self.dump()
    }

    fn target(&self) -> IpAddr {
        self.target
    }

    fn whoami(&self) -> PollType {
        PollType::Ping
    }

    fn username(&self) -> String {
        self.name()
    }
}
