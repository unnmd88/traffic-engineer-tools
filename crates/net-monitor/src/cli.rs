use clap::{Parser, Subcommand};

/// Сетевая диагностическая утилита
#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version)]
#[command(about = env!("CARGO_PKG_DESCRIPTION"), long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Запускает монитор
    Start {
        /// Путь к конфиг-файлу
        #[arg(short, long, default_value = "config.toml")]
        config: String,
    },

    /// Сгенерировать дефолтный конфиг
    GenerateConfig {
        /// Путь для сохранения
        #[arg(short, long, default_value = "config.toml")]
        output: String,

        /// Принудительно перезаписать существующий файл
        #[arg(short, long, action)]
        force: bool,

        /// Показать дефолтный конфиг в консоли (без сохранения)
        #[arg(long, short = 's', action)]
        show: bool,
    },

    ProcessLog {
        /// Путь к файлу лога для чтения
        #[arg(short, long, default_value = "monitor.json")]
        log: String,

        /// Формат вывода: csv, console
        format: OutputFormat,
    },
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Csv,
    Console,
}
