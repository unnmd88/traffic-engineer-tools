use tokio::time::Duration;

use super::error::ConfigError;

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
}
