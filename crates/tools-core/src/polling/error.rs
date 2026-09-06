use std::fmt::{self, Display, Formatter};
use thiserror::Error;
use tokio::time::Duration;

// Ошибка одной попытки опроса: классифицирует АДАПТЕР.
#[derive(Error, Debug, Clone)]
pub enum AttemptError {
    /// Сеть/таймаут/недоступно — можно повторить.
    #[error("transient error: {0}")]
    Transient(String),
    /// Баг/конфиг/внутренняя — ретраи не помогут.
    #[error("fatal error: {0}")]
    Fatal(String),
}

// Фатальная ошибка итерации опроса (пробрасывается наверх).
#[derive(Error, Debug, Clone)]
#[error("fatal poll error: {message}")]
pub struct FatalError {
    pub message: String,
}

// Контекст одной неудачной попытки.
#[derive(Debug, Clone)]
pub struct PollErrorContext {
    pub attempt: u8,
    pub elapsed: Duration,
    pub message: String,
}

impl Display for PollErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "attempt: {} elapsed_ms: {} message: {}",
            self.attempt,
            self.elapsed.as_millis(),
            &self.message
        )?;
        Ok(())
    }
}
