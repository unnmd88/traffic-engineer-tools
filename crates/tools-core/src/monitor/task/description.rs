use serde::{Deserialize, Serialize};

use crate::monitor::ScenarioDescription;

use super::TaskConfig;

/// A task configuration captured for observation, excluding network credentials.
///
/// Event metadata supplies the task identity and accepted configuration revision.
/// Later configuration changes do not mutate an already-captured description.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TaskDescription {
    /// The configured task name.
    pub name: String,
    /// The scenario's request parameters and behavior.
    pub scenario: ScenarioDescription,
    /// The configured complete-operation limit; zero means no limit.
    pub limit: u64,
    /// The configured number of previous results to retain.
    pub deep_history: u8,
}

impl From<&TaskConfig> for TaskDescription {
    fn from(config: &TaskConfig) -> Self {
        Self {
            name: config.name().to_string(),
            scenario: ScenarioDescription::from(config.scenario()),
            limit: config.limit(),
            deep_history: config.deep_history(),
        }
    }
}
