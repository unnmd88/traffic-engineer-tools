use tokio::time::{Duration, Instant};

use crate::{
    monitor::probe::BuiltProbe,
    polling::{AttemptConfig, FatalError, poll},
};

use super::Outcome;

pub struct IntervalMode {
    probe: BuiltProbe,
    interval: Duration,
    attempt: AttemptConfig,
}

impl IntervalMode {
    pub fn new(probe: BuiltProbe, interval: Duration, attempt: AttemptConfig) -> Self {
        Self {
            probe,
            interval,
            attempt,
        }
    }

    pub fn next_deadline(&self) -> Instant {
        Instant::now() + self.interval
    }

    pub async fn do_work(&self) -> Result<Option<Outcome>, FatalError> {
        poll(&self.attempt, &self.probe)
            .await
            .map(|r| Some(Outcome::Polled(r)))
    }
}
