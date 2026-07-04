use serde::Deserialize;
use std::net::IpAddr;
use tracing::info;

use crate::models::TracerouteEngine;

// ============================================
// CONFIG
// ============================================
#[derive(Debug, Deserialize)]
pub struct Config {
    pub log: String,
    pub independent_enabled: bool,
    pub synchronized_enabled: bool,
    pub independent: IndependentStrategyConfig,
    pub synchronized: SynchronizedStrategyConfig,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            format!("❌ Не удалось прочитать '{}': {}", path, e)
        })?;

        let config: Config = toml::from_str(&contents).map_err(|e| {
            format!("❌ Ошибка в конфиге: {}", user_friendly_error(&e))
        })?;

        info!("Config '{}' loaded successfully.", path);
        Ok(config)
    }

    pub fn generate_default(
        output: &str,
        force: bool,
        show: bool,
    ) -> Result<(), String> {
        let default = r#"log = "monitor.json"
independent_enabled = true
synchronized_enabled = true

# ============================================
# INDEPENDENT STRATEGY
# ============================================

# ---------- PING ----------
[[independent.ping]]
enabled = true
name = "fast"
target = "10.179.180.190"
interval_ms = 1000
timeout_ms = 200
retries = 3
retries_delay_ms = 200

# fallback traceroute ВКЛЮЧЕН
[independent.ping.fallback_tracert]
type = "library"
max_hops = 30
queries_per_hop = 1
timeout_ms = 1000

[[independent.ping]]
enabled = true
name = "slow"
target = "8.8.8.8"
interval_ms = 5000
timeout_ms = 500
retries = 5
retries_delay_ms = 500
# fallback_tracert НЕТ → выключен

# ---------- SNMP ----------
[[independent.snmp]]
enabled = true
name = "main"
target = "10.179.180.190"
interval_ms = 6000
timeout_ms = 350
port = 161
community = "UTMC"
retries = 2
retries_delay_ms = 300
oids = ["1.3.6.1.2.1.1.3.0"]

# fallback traceroute ВКЛЮЧЕН
[independent.snmp.fallback_tracert]
type = "system"
max_hops = 30
queries_per_hop = 1
timeout_ms = 2000

# ---------- TRACERT ----------
[[independent.tracert]]
enabled = true
name = "default"
target = "10.179.180.190"
interval_ms = 30000
max_hops = 30
queries_per_hop = 1

# ============================================
# SYNCHRONIZED STRATEGY
# ============================================

[synchronized]
enabled = true
interval_ms = 4000

# ---------- PING ----------
[[synchronized.ping]]
enabled = true
name = "sync-ping"
target = "10.179.180.190"
timeout_ms = 200
retries = 3
retries_delay_ms = 200

[synchronized.ping.fallback_tracert]
type = "library"
max_hops = 30
queries_per_hop = 1
timeout_ms = 1000

# ---------- SNMP ----------
[[synchronized.snmp]]
enabled = true
name = "sync-snmp"
target = "10.179.180.190"
timeout_ms = 350
port = 161
community = "UTMC"
retries = 2
retries_delay_ms = 300
oids = ["1.3.6.1.2.1.1.3.0"]

# ---------- TRACERT ----------
[[synchronized.tracert]]
enabled = true
name = "sync-tracert"
target = "10.179.180.190"
max_hops = 30
queries_per_hop = 1
"#;

        if show {
            println!("{}", default);
            return Ok(());
        }

        if std::fs::metadata(output).is_ok() && !force {
            return Err(format!(
                "❌ Файл '{}' уже существует.\n💡 Используйте --force для перезаписи.",
                output
            ));
        }

        std::fs::write(output, default).map_err(|e| {
            format!("❌ Не удалось записать '{}': {}", output, e)
        })?;

        println!("✅ Дефолтный конфиг создан: {}", output);
        Ok(())
    }
}

// ============================================
// BASE PROVIDER CONFIGS
// ============================================

#[derive(Debug, Deserialize, Clone)]
pub struct PingConfig {
    pub name: String,
    pub target: IpAddr,
    pub timeout_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SnmpConfig {
    pub name: String,
    pub target: IpAddr,
    pub timeout_ms: u64,
    pub port: u16,
    pub community: String,
    pub oids: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TracertConfig {
    pub name: String,
    pub target: IpAddr,
    pub engine: TracerouteEngine,
    pub max_hops: u8,
    pub queries_per_hop: u8,
    pub timeout_ms: u64,
}

// ============================================
// PROVIDER ENUM
// ============================================

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Provider {
    Ping(PingConfig),
    Snmp(SnmpConfig),
    Tracert(TracertConfig),
}

// ============================================
// INDEPENDENT STRATEGY
// ============================================

#[derive(Debug, Deserialize)]
pub struct IndependentPingInstance {
    pub enabled: bool,
    pub interval_ms: u64,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: PingConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize)]
pub struct IndependentSnmpInstance {
    pub enabled: bool,
    pub interval_ms: u64,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: SnmpConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize)]
pub struct IndependentTracertInstance {
    pub enabled: bool,
    pub interval_ms: u64,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: TracertConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize, Default)]
pub struct IndependentStrategyConfig {
    #[serde(default)]
    pub ping: Vec<IndependentPingInstance>,
    #[serde(default)]
    pub snmp: Vec<IndependentSnmpInstance>,
    #[serde(default)]
    pub tracert: Vec<IndependentTracertInstance>,
}

// ============================================
// SYNCHRONIZED STRATEGY
// ============================================

#[derive(Debug, Deserialize)]
pub struct SynchronizedPingInstance {
    pub enabled: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: PingConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize)]
pub struct SynchronizedSnmpInstance {
    pub enabled: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: SnmpConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize)]
pub struct SynchronizedTracertInstance {
    pub enabled: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
    #[serde(flatten)]
    pub config: TracertConfig,
    #[serde(default)]
    pub fallback: Option<Provider>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SynchronizedStrategyConfig {
    pub interval_ms: u64,
    #[serde(default)]
    pub ping: Vec<SynchronizedPingInstance>,
    #[serde(default)]
    pub snmp: Vec<SynchronizedSnmpInstance>,
    #[serde(default)]
    pub tracert: Vec<SynchronizedTracertInstance>,
}

// ============================================
// HELPERS
// ============================================
fn user_friendly_error(e: &toml::de::Error) -> String {
    let msg = e.message();

    if msg.contains("missing field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!("отсутствует поле '{}'", field);
        }
        return "отсутствует обязательное поле".to_string();
    }

    if msg.contains("unknown field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!(
                "неизвестное поле '{}' — проверьте название",
                field
            );
        }
    }

    if msg.contains("invalid type") {
        return "неверный тип значения (например, строка вместо числа)"
            .to_string();
    }

    msg.to_string()
}
