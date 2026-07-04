use tracing_appender::non_blocking::WorkerGuard;

pub fn init_tracing() -> WorkerGuard {
    let file_appender = tracing_appender::rolling::never(".", "app.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_env_filter("info,async_snmp=off")
        .with_writer(non_blocking)
        .with_target(false)
        .init();

    guard
}
