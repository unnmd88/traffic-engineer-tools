use crate::monitor::scenario::ScenarioSpec;

use super::error::TaskError;

#[derive(Clone, Debug)]
pub struct TaskConfig {
    name: String,
    scenario: ScenarioSpec,
    limit: u64,
    deep_history: u8,
}

impl TaskConfig {
    pub fn try_new(
        name: String,
        scenario: ScenarioSpec,
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

    pub const fn scenario(&self) -> &ScenarioSpec {
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
        monitor::scenario::{PollSchedule, SnmpGetScenarioSpec},
        polling::AttemptConfig,
        snmp::SnmpGetSpec,
    };
    use tokio::time::Duration;

    fn valid_scenario() -> ScenarioSpec {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        ScenarioSpec::SnmpGet(
            SnmpGetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(1),
                },
                attempt,
            )
            .unwrap(),
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
