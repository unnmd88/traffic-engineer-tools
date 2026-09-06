use chrono::{DateTime, Local};
use tokio::time::Duration;

use crate::polling::PollErrorContext;

// Итог одной итерации опроса (с ретраями): успех ИЛИ «нет ответа» — оба штатные (value).
#[derive(Debug, Clone)]
pub enum Response<T> {
    Success {
        timestamp: DateTime<Local>,
        attempts: u8,
        errors: Vec<PollErrorContext>,
        elapsed: Duration,
        payload: T,
    },
    NoResponse {
        timestamp: DateTime<Local>,
        attempts: u8,
        errors: Vec<PollErrorContext>,
        elapsed: Duration,
    },
}
