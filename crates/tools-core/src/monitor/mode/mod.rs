mod coordination;
mod interval;

use tokio::time::Instant;

use crate::{
    monitor::probe::ProbeOutput,
    polling::{FatalError, Response},
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
