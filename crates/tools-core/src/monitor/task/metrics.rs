use tokio::time::Duration;

use crate::monitor::probe::OutcomeStatus;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionMetrics {
    pub executions: u64,
    pub successful: u64,
    pub degraded: u64,
    pub failed: u64,
    pub current_duration: Duration,
    pub min_duration: Option<Duration>,
    pub max_duration: Option<Duration>,
}

impl ExecutionMetrics {
    pub fn record(&mut self, status: OutcomeStatus, elapsed: Duration) {
        self.executions += 1;
        match status {
            OutcomeStatus::Succeeded => self.successful += 1,
            OutcomeStatus::Degraded => self.degraded += 1,
            OutcomeStatus::Failed => self.failed += 1,
        }
        self.current_duration = elapsed;
        self.min_duration = Some(self.min_duration.map_or(elapsed, |min| min.min(elapsed)));
        self.max_duration = Some(self.max_duration.map_or(elapsed, |max| max.max(elapsed)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_all_outcomes_and_measures_whole_executions() {
        let mut metrics = ExecutionMetrics::default();
        assert_eq!(metrics.min_duration, None);
        assert_eq!(metrics.max_duration, None);

        metrics.record(OutcomeStatus::Succeeded, Duration::from_millis(20));
        metrics.record(OutcomeStatus::Degraded, Duration::from_millis(10));
        metrics.record(OutcomeStatus::Failed, Duration::from_millis(50));

        assert_eq!(metrics.executions, 3);
        assert_eq!(metrics.successful, 1);
        assert_eq!(metrics.degraded, 1);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.current_duration, Duration::from_millis(50));
        assert_eq!(metrics.min_duration, Some(Duration::from_millis(10)));
        assert_eq!(metrics.max_duration, Some(Duration::from_millis(50)));
    }
}
