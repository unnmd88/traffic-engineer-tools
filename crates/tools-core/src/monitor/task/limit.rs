use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

/// The number of completed runs allowed by one task start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunLimit {
    /// The task can keep running until stopped or until the scenario completes.
    Unlimited,
    /// The task completes after this many accounted runs.
    Runs(NonZeroU64),
}
