use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use super::error::ConfigError;

/// Timeout and retry settings for one complete operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AttemptConfigData")]
pub struct AttemptConfig {
    timeout: Duration,
    retries: u8,
    retry_delay: Duration,
}

#[derive(Deserialize)]
struct AttemptConfigData {
    timeout: Duration,
    retries: u8,
    retry_delay: Duration,
}

impl TryFrom<AttemptConfigData> for AttemptConfig {
    type Error = ConfigError;

    fn try_from(value: AttemptConfigData) -> Result<Self, Self::Error> {
        Self::try_new(value.timeout, value.retries, value.retry_delay)
    }
}

impl AttemptConfig {
    /// Creates validated attempt settings.
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

    /// Returns the timeout of each individual attempt.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the number of retries after the first attempt.
    pub fn retries(&self) -> u8 {
        self.retries
    }

    /// Returns the delay between unsuccessful attempts.
    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    /// Estimates the operation duration from attempt timeouts and retry delays.
    pub fn budget(&self) -> Duration {
        let attempts = self.retries as u32 + 1;
        self.timeout * attempts + self.retry_delay * (self.retries as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_settings_preserve_validation() {
        let config =
            AttemptConfig::try_new(Duration::from_secs(1), 255, Duration::from_millis(100))
                .unwrap();
        let mut json = serde_json::to_value(config).unwrap();
        assert_eq!(serde_json::from_value::<AttemptConfig>(json.clone()).unwrap(), config);
        json["timeout"] = serde_json::json!({ "secs": 0, "nanos": 0 });
        assert!(serde_json::from_value::<AttemptConfig>(json).is_err());
    }

    #[test]
    fn rejects_zero_timeout() {
        let err = AttemptConfig::try_new(Duration::from_millis(0), 0, Duration::from_millis(10))
            .unwrap_err();
        assert!(matches!(err, ConfigError::TimeoutMustBePositive));
    }
}
