use tokio::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    pub total_attempts: u64,
    pub successful: u64,
    pub errors: u64,
    pub current_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_attempts: 0,
            successful: 0,
            errors: 0,
            current_latency_ms: 0,
            min_latency_ms: u64::MAX,
            max_latency_ms: 0,
        }
    }

    pub fn with_success(self, latency: Duration) -> Self {
        let current_latency_ms = latency.as_millis() as u64;
        let new_successful = self.successful + 1;

        Self {
            total_attempts: self.total_attempts + 1,
            successful: new_successful,
            errors: self.errors,
            current_latency_ms,
            min_latency_ms: self.min_latency_ms.min(current_latency_ms),
            max_latency_ms: self.max_latency_ms.max(current_latency_ms),
        }
    }

    pub fn with_error(self) -> Self {
        Self {
            total_attempts: self.total_attempts + 1,
            successful: self.successful,
            errors: self.errors + 1,
            ..self
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new()
    }
}
