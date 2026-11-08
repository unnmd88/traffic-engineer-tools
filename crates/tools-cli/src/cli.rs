use std::{fmt::Display, path::PathBuf};

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

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum PollView {
    #[default]
    Classic,
    Compact,
}

#[derive(Subcommand)]
pub enum Commands {
    Poll {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(long, value_enum, default_value_t = PollView::Classic)]
        view:   PollView,
    },

    ToScn {
        input:  String,
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },

    FromScn {
        input:  String,
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },
}

/// Универсальная функция для вывода результата в нужном формате
pub fn print_output<T: Display + Serialize>(format: OutputFormat, value: &T) -> Result<()> {
    match format {
        OutputFormat::Human => println!("{value}"),
        OutputFormat::Json => println!("{}", serde_json::to_string(value)?),
        OutputFormat::PrettyJson => println!("{}", serde_json::to_string_pretty(value)?),
    }
    Ok(())
}
