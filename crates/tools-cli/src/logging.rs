use std::env::{self, VarError};

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_LOG_FILTER: &str = "warn,tools_core::monitor=info,tctl=info";

/// Инициализирует логирование в файл.
///
/// # Аргументы
/// * `log_dir` - директория для логов (например, "logs")
/// * `file_prefix` - префикс для файлов логов (например, "traffic")
pub fn init_file_logging(log_dir: &str, file_prefix: &str) -> anyhow::Result<WorkerGuard> {
    let directives = match env::var(EnvFilter::DEFAULT_ENV) {
        Ok(value) => value,
        Err(VarError::NotPresent) => DEFAULT_LOG_FILTER.to_owned(),
        Err(error) => return Err(error).context("failed to read RUST_LOG"),
    };
    let filter = EnvFilter::try_new(directives).context("invalid RUST_LOG filter")?;

    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log directory: {log_dir}"))?;

    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::NEVER)
        .filename_prefix(format!("{file_prefix}.log"))
        .build(log_dir)
        .with_context(|| format!("failed to open log file: {log_dir}/{file_prefix}.log"))?;
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::Layer::new()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true),
        )
        .try_init()
        .context("failed to initialize tracing subscriber")?;

    Ok(guard)
}
