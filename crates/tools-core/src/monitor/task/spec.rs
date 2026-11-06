use chrono::{DateTime, Utc};
use derive_more::{Constructor, Display};

use crate::monitor::scenario::Scenario;

use super::error::TaskError;

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor)]
pub struct SpecRevision(pub u64);

impl SpecRevision {
    pub fn next(self) -> Self {
        Self::new(self.0 + 1)
    }
}

#[derive(Clone, Debug)]
pub struct TaskConfig {
    name: String,
    scenario: Scenario,
    limit: u64,
    deep_history: u8,
}

#[derive(Clone, Debug)]
pub struct TaskSpec {
    value: TaskConfig,
    revision: SpecRevision,
    updated_at: DateTime<Utc>,
}

impl TaskSpec {
    pub fn new(spec: TaskConfig) -> Self {
        Self {
            value: spec,
            revision: SpecRevision::new(1),
            updated_at: Utc::now(),
        }
    }

    pub fn next(&self, spec: TaskConfig) -> Self {
        Self {
            value: spec,
            revision: self.revision.next(),
            updated_at: Utc::now(),
        }
    }

    pub fn name(&self) -> &str {
        &self.value.name()
    }

    pub const fn scenario(&self) -> &Scenario {
        self.value.scenario()
    }

    pub const fn limit(&self) -> u64 {
        self.value.limit()
    }

    pub fn deep_history(&self) -> u8 {
        self.value.deep_history()
    }

    pub fn revision(&self) -> SpecRevision {
        self.revision
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

impl TaskConfig {
    pub fn try_new(
        name: String,
        scenario: Scenario,
        limit: u64,
        deep_history: u8,
    ) -> Result<Self, TaskError> {
        if name.trim().is_empty() {
            return Err(TaskError::EmptyName);
        }

        Ok(Self {
            name,
            scenario,
            limit,
            deep_history,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    pub const fn limit(&self) -> u64 {
        self.limit
    }

    pub fn deep_history(&self) -> u8 {
        self.deep_history
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::{probe::SnmpGetSpec, scenario::SnmpGetScenarioSpec},
        polling::AttemptConfig,
    };
    use tokio::time::Duration;

    fn valid_scenario() -> Scenario {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        Scenario::SnmpGet(
            SnmpGetScenarioSpec::try_new(request, Duration::from_secs(1), attempt).unwrap(),
        )
    }

    #[test]
    fn rejects_empty_name() {
        let err = TaskConfig::try_new("   ".to_string(), valid_scenario(), 0, 3).unwrap_err();
        assert!(matches!(err, TaskError::EmptyName));
    }

    #[test]
    fn accepts_valid_spec() {
        let spec = TaskConfig::try_new("T-1".to_string(), valid_scenario(), 100, 3).unwrap();
        assert_eq!(spec.name(), "T-1");
        assert_eq!(spec.limit(), 100);
        assert_eq!(spec.deep_history(), 3);
    }
}
