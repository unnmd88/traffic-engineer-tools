use chrono::{DateTime, Local};
use derive_more::{Constructor, Display};

use crate::monitor::mode::ModeSpec;

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
    mode: ModeSpec,
    deep_history: u8,
}

#[derive(Clone, Debug)]
pub struct TaskSpec {
    value: TaskConfig,
    revision: SpecRevision,
    updated_at: DateTime<Local>,
}

impl TaskSpec {
    pub fn new(spec: TaskConfig) -> Self {
        Self {
            value: spec,
            revision: SpecRevision::new(1),
            updated_at: Local::now(),
        }
    }

    pub fn next(&self, spec: TaskConfig) -> Self {
        Self {
            value: spec,
            revision: self.revision.next(),
            updated_at: Local::now(),
        }
    }

    pub fn name(&self) -> &str {
        &self.value.name()
    }

    pub fn mode(&self) -> &ModeSpec {
        self.value.mode()
    }

    pub fn deep_history(&self) -> u8 {
        self.value.deep_history()
    }

    pub fn revision(&self) -> SpecRevision {
        self.revision
    }

    pub fn updated_at(&self) -> DateTime<Local> {
        self.updated_at
    }
}

impl TaskConfig {
    pub fn try_new(name: String, mode: ModeSpec, deep_history: u8) -> Result<Self, TaskError> {
        if name.trim().is_empty() {
            return Err(TaskError::EmptyName);
        }

        Ok(Self {
            name,
            mode,
            deep_history,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mode(&self) -> &ModeSpec {
        &self.mode
    }

    pub fn deep_history(&self) -> u8 {
        self.deep_history
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::{
            mode::ModeSpec,
            probe::{ProbeSpec, SnmpGetSpec},
        },
        polling::{AttemptConfig, PollConfig},
    };
    use tokio::time::Duration;

    fn valid_mode() -> ModeSpec {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let poll = PollConfig::try_new(Duration::from_secs(1), 0, attempt).unwrap();
        let probe = ProbeSpec::SnmpGet(
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap(),
        );
        ModeSpec::Interval { probe, poll }
    }

    #[test]
    fn rejects_empty_name() {
        let err = TaskConfig::try_new("   ".to_string(), valid_mode(), 3).unwrap_err();
        assert!(matches!(err, TaskError::EmptyName));
    }

    #[test]
    fn accepts_valid_spec() {
        let spec = TaskConfig::try_new("T-1".to_string(), valid_mode(), 3).unwrap();
        assert_eq!(spec.name(), "T-1");
        assert_eq!(spec.deep_history(), 3);
    }
}
