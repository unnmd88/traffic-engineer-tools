use chrono::{FixedOffset, Local, SecondsFormat, Utc};
use uuid::Uuid;

use crate::constants::TIME_FMT;

pub fn get_fmt_current_time() -> String {
    Local::now()
        .format(TIME_FMT)
        .to_string()
}

pub fn get_session_id() -> String {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(utime) => format!("{:012x}", utime.as_nanos() & 0xFFFFFFFFFFFF),
        Err(_) => Uuid::new_v4().to_string(),
    }
}

pub fn get_timestamp_fmt() -> String {
    let msk_offset = match FixedOffset::east_opt(3 * 3600) {
        Some(offset) => offset,
        None => return "1970-01-01T00:00:00.000+00:00".to_string(),
    };
    Utc::now()
        .with_timezone(&msk_offset)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}
