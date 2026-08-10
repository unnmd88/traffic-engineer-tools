use std::{net::IpAddr, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum, builder::Str};
use serde::Serialize;
use tools_core::snmp::primitives::{Community, SnmpOid};

#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// Формат вывода
#[derive(Debug, Clone, ValueEnum, Default)]
pub enum OutputFormat {
    /// Человекочитаемый текст (по умолчанию)
    #[default]
    Human,
    /// JSON
    Json,
    /// Pretty JSON
    PrettyJson,
}

fn parse_snmp_oid(s: &str) -> anyhow::Result<SnmpOid> {
    Ok(SnmpOid::parse(s)?)
}

#[derive(Subcommand)]
pub enum Commands {
    Poll {
        #[arg(short, long)]
        config: PathBuf,
    },
    /*
    ToAscii {
        input: String,
    },
    */
    Probe {
        /// Путь к конфиг-файлу YAML
        #[arg(long)]
        config: Option<PathBuf>,

        /// Сгенерировать дефолтный конфиг
        #[arg(long)]
        gen_config: bool,
    },
    ProbeSnmpGetStage {
        // TODO!

        // --- Параметры для быстрой проверки (одна проба) ---
        /// IP-адрес устройства
        #[arg(long)]
        address: IpAddr,

        /// OID'ы для опроса (можно указать несколько раз)
        #[arg(long, value_parser = parse_snmp_oid)]
        oids: Vec<SnmpOid>,

        /// SNMP порт (по умолчанию 161)
        #[arg(long)]
        port: u16,

        /// SNMP community (по умолчанию "public")
        #[arg(long)]
        community: Community,

        /// Таймаут в миллисекундах (по умолчанию 5000)
        #[arg(long)]
        timeout: u64,

        /// Количество попыток (по умолчанию 3)
        #[arg(long)]
        retries: u32,

        /// Задержка между попытками в мс (по умолчанию 1000)
        #[arg(long)]
        retry_delay: u64,
    },

    ToScn {
        input: String,

        #[arg(short, long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },
    FromScn {
        input: String,
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Protocol {
    Snmp,
    Modbus,
    Http,
}

/// Универсальная функция для вывода результата в нужном формате
pub fn print_output<T: Serialize>(format: OutputFormat, text: &str, data: &T) -> Result<()> {
    match format {
        OutputFormat::Human => {
            println!("{}", text);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string(data)?);
        }
        OutputFormat::PrettyJson => {
            println!("{}", serde_json::to_string_pretty(data)?);
        }
    }
    Ok(())
}
