use tokio::time::{Duration, Instant};

const INFO_DEPTH: usize = 64;
pub(super) const WARNING_DEPTH: usize = 256;
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct QueuePressure {
    peak_depth: usize,
    last_report: Option<Instant>,
}

impl QueuePressure {
    pub(super) fn sample(&mut self, depth: usize) -> Option<usize> {
        if depth <= INFO_DEPTH {
            *self = Self::default();
            return None;
        }

        let crossed_warning = self.peak_depth <= WARNING_DEPTH && depth > WARNING_DEPTH;
        self.peak_depth = self.peak_depth.max(depth);
        let now = Instant::now();
        if !crossed_warning
            && self
                .last_report
                .is_some_and(|last| now - last < REPORT_INTERVAL)
        {
            return None;
        }

        self.last_report = Some(now);
        Some(self.peak_depth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn reports_threshold_crossings_and_sustained_pressure() {
        let mut pressure = QueuePressure::default();
        assert_eq!(pressure.sample(INFO_DEPTH), None);
        assert_eq!(pressure.sample(INFO_DEPTH + 1), Some(INFO_DEPTH + 1));
        assert_eq!(pressure.sample(WARNING_DEPTH), None);
        assert_eq!(pressure.sample(WARNING_DEPTH + 1), Some(WARNING_DEPTH + 1));
        assert_eq!(pressure.sample(WARNING_DEPTH + 2), None);

        tokio::time::advance(REPORT_INTERVAL).await;
        assert_eq!(pressure.sample(WARNING_DEPTH + 1), Some(WARNING_DEPTH + 2));

        assert_eq!(pressure.sample(INFO_DEPTH), None);
        assert_eq!(pressure.sample(INFO_DEPTH + 1), Some(INFO_DEPTH + 1));
    }
}
