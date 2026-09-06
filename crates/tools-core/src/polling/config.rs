use tokio::time::Duration;

use crate::error::ConfigError;

/// Расписание и лимит одного воркера.
///
/// Создаётся только через [`PollConfig::try_new`] — невалидные значения
/// невыразимы (поля приватные).
#[derive(Clone, Copy, Debug)]
pub struct PollConfig {
    interval: Duration,
    limit: u64,
    attempt: AttemptConfig,
}

impl PollConfig {
    pub fn try_new(
        interval: Duration,
        limit: u64,
        attempt: AttemptConfig,
    ) -> Result<Self, ConfigError> {
        if interval.is_zero() {
            return Err(ConfigError::IntervalMustBePositive);
        }

        // Воркер обязан укладываться в интервал: иначе он не держит ритм.
        let budget = attempt.budget();
        if interval < budget {
            return Err(ConfigError::IntervalShorterThanAttemptBudget {
                interval_ms: interval.as_millis() as u64,
                budget_ms: budget.as_millis() as u64,
            });
        }

        Ok(Self {
            interval,
            limit,
            attempt,
        })
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// `0` — без лимита.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn attempt(&self) -> AttemptConfig {
        self.attempt
    }
}

/// Параметры одной итерации опроса.
#[derive(Clone, Copy, Debug)]
pub struct AttemptConfig {
    timeout: Duration,
    retries: u8,
    retry_delay: Duration,
}

impl AttemptConfig {
    pub fn try_new(
        timeout: Duration,
        retries: u8,
        retry_delay: Duration,
    ) -> Result<Self, ConfigError> {
        if timeout.is_zero() {
            return Err(ConfigError::TimeoutMustBePositive);
        }
        Ok(Self {
            timeout,
            retries,
            retry_delay,
        })
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Число повторов ПОСЛЕ первой попытки (итого попыток = 1 + retries).
    pub fn retries(&self) -> u8 {
        self.retries
    }

    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    /// Worst-case время одной итерации: все попытки по таймауту + паузы между ними.
    pub fn budget(&self) -> Duration {
        let attempts = self.retries as u32 + 1;
        self.timeout * attempts + self.retry_delay * (self.retries as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(timeout_ms: u64, retries: u8, retry_delay_ms: u64) -> AttemptConfig {
        AttemptConfig::try_new(
            Duration::from_millis(timeout_ms),
            retries,
            Duration::from_millis(retry_delay_ms),
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_timeout() {
        let err = AttemptConfig::try_new(
            Duration::from_millis(0),
            0,
            Duration::from_millis(10),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::TimeoutMustBePositive));
    }

    #[test]
    fn rejects_zero_interval() {
        let err = PollConfig::try_new(Duration::from_millis(0), 0, attempt(100, 0, 0)).unwrap_err();
        assert!(matches!(err, ConfigError::IntervalMustBePositive));
    }

    #[test]
    fn rejects_interval_shorter_than_budget() {
        // budget = 100*(2+1) + 50*2 = 400ms
        let err =
            PollConfig::try_new(Duration::from_millis(300), 0, attempt(100, 2, 50)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::IntervalShorterThanAttemptBudget { .. }
        ));
    }

    #[test]
    fn accepts_interval_equal_to_budget() {
        let cfg = PollConfig::try_new(Duration::from_millis(400), 0, attempt(100, 2, 50)).unwrap();
        assert_eq!(cfg.interval(), Duration::from_millis(400));
    }
}
