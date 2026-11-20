use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A scenario's human-readable preparation activity, independent of lifecycle transitions.
///
/// Messages are for display, may change, and must not be parsed as stable identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgress {
    /// UTC time at which the scenario reported this activity.
    pub at: DateTime<Utc>,
    /// The current activity without credentials or other secrets.
    pub message: String,
}
