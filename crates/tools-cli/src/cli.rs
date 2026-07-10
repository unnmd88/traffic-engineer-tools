use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

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

#[derive(Subcommand)]
pub enum Commands {
    /*
    Poll {
        protocol: Protocol,
        host: String,
        #[arg(short, long)]
        config: Option<String>,
    },
    ToAscii {
        input: String,
    },
    */
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
