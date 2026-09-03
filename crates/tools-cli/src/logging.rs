use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Инициализирует логирование в файл (с ежедневной ротацией).
///
/// # Аргументы
/// * `log_dir` - директория для логов (например, "logs")
/// * `file_prefix` - префикс для файлов логов (например, "traffic")
pub fn init_file_logging(log_dir: &str, file_prefix: &str) -> anyhow::Result<WorkerGuard> {
    std::fs::create_dir_all(log_dir)?;

    let file_appender = tracing_appender::rolling::never(log_dir, format!("{}.log", file_prefix));
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    // Явно устанавливаем уровень INFO (или TRACE для отладки)
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::Layer::new().with_writer(non_blocking).with_ansi(false))
        .init();

    Ok(guard)
}

/// Инициализирует логирование в консоль.
///
/// # Пример
/// ```
/// init_console_logging();
/// ```
pub fn init_console_logging() {
    tracing_subscriber::fmt::init();
}
