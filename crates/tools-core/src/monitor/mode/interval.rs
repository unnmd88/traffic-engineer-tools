use tokio::time::{Duration, Instant};

use crate::{
    monitor::probe::{BuiltProbe, ProbeOutput},
    polling::{AttemptConfig, FatalError, Response, poll},
};

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

    pub async fn do_work(&self) -> Result<Response<ProbeOutput>, FatalError> {
        poll(&self.attempt, &self.probe).await
    }
}
