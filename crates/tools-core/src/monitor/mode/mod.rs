mod coordination;
mod interval;

use tokio::time::{Duration, Instant};

use crate::{
    monitor::probe::{self, ProbeBuildError, ProbeOutput, ProbeSpec},
    polling::{AttemptConfig, FatalError, PollConfig, Response},
};

pub use coordination::{
    CoordinationError, CoordinationMode, Cycle, Epoch, Phase, PhaseSetResult, PhaseState,
};
pub use interval::IntervalMode;

pub enum Outcome {
    Polled(Response<ProbeOutput>),
    PhaseSet(PhaseSetResult),
}

pub enum Mode {
    Interval(IntervalMode),
    Coordination(CoordinationMode),
}

impl Mode {
    pub fn next_deadline(&self) -> Instant {
        match self {
            Mode::Interval(m) => m.next_deadline(),
            Mode::Coordination(m) => m.next_deadline(),
        }
    }

    pub async fn do_work(&self) -> Result<Option<Outcome>, FatalError> {
        match self {
            Mode::Interval(m) => m.do_work().await,
            Mode::Coordination(m) => m.do_work().await,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ModeSpec {
    Interval { probe: ProbeSpec, poll: PollConfig },
}

impl ModeSpec {
    pub fn probe(&self) -> &ProbeSpec {
        match self {
            ModeSpec::Interval { probe, .. } => probe,
        }
    }

    pub fn interval(&self) -> Duration {
        match self {
            ModeSpec::Interval { poll, .. } => poll.interval(),
        }
    }

    pub fn limit(&self) -> u64 {
        match self {
            ModeSpec::Interval { poll, .. } => poll.limit(),
        }
    }

    pub fn attempt(&self) -> AttemptConfig {
        match self {
            ModeSpec::Interval { poll, .. } => poll.attempt(),
        }
    }

    pub fn build_timeout(&self) -> Duration {
        match self {
            ModeSpec::Interval { poll, .. } => probe::build_timeout(&poll.attempt()),
        }
    }

    pub async fn build(self) -> Result<Mode, ProbeBuildError> {
        match self {
            ModeSpec::Interval { probe, poll } => {
                let built = probe.build(poll.attempt()).await?;
                Ok(Mode::Interval(IntervalMode::new(
                    built,
                    poll.interval(),
                    poll.attempt(),
                )))
            }
        }
    }
}
