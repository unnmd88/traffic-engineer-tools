// pollers/time.rs
use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use tokio::time::Instant;

const MOSCOW_OFFSET: i32 = 3;

fn get_offset() -> FixedOffset {
    FixedOffset::east_opt(MOSCOW_OFFSET * 3600).unwrap_or_else(|| FixedOffset::east_opt(0).unwrap())
}

// Для человека (логи, вывод)
pub fn format_moscow_human(dt: &DateTime<Utc>) -> String {
    dt.with_timezone(&get_offset())
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

// Для машины (API, JSON)
pub fn format_moscow_machine(dt: &DateTime<Utc>) -> String {
    dt.with_timezone(&get_offset())
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn now_moscow_human() -> String {
    format_moscow_human(&Utc::now())
}

pub fn now_moscow_machine() -> String {
    format_moscow_machine(&Utc::now())
}

pub fn get_elapsed_as_u64(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}
