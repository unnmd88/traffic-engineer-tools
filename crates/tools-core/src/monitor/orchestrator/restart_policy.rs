use std::time::Duration;

/// Чистая логика backoff: задержка рестарта по номеру попытки.
pub struct RestartPolicy {
    backoff_base: Duration,
    backoff_max: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
        }
    }
}

impl RestartPolicy {
    pub fn delay(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(6);
        self.backoff_base
            .saturating_mul(1u32 << shift)
            .min(self.backoff_max)
    }
}
