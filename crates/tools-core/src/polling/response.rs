use std::fmt::{self, Display, Formatter};
use tokio::time::Duration;

use chrono::{DateTime, Local};

use crate::error::PollErrorContext;

#[derive(Debug, Clone)]
pub struct Response<T> {
    pub timestamp: DateTime<Local>,
    pub attempts: u8,
    pub errors: Vec<PollErrorContext>,
    pub elapsed: Duration,
    pub payload: T,
}

impl<T: Display> Display for Response<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Время ответа: {}", self.timestamp)?;
        writeln!(f, "Время на запрос составило: {}ms", self.elapsed.as_millis())?;
        for err in self.errors.iter() {
            writeln!(f, "{err}")?;
        }
        writeln!(f, "{}", self.payload)?;
        Ok(())
    }
}
