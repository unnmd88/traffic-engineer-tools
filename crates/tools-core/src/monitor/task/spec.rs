use chrono::{DateTime, Local};

use crate::{
    monitor::{adapter::Query, task::SpecRevision},
    polling::PollConfig,
};

use super::error::TaskError;

/// Валидированное декларативное описание (Spec) — неизменяемый value object.
#[derive(Clone, Debug)]
pub struct TaskConfig {
    name: String,
    query: Query,
    poll_config: PollConfig,
    deep_history: u8,
}

/// Спека + версия и время постановки
#[derive(Clone, Debug)]
pub struct TaskSpec {
    value: TaskConfig,
    revision: SpecRevision,
    updated_at: DateTime<Local>,
}

impl TaskSpec {
    pub fn new(payload: TaskConfig) -> Self {
        Self {
            value: payload,
            revision: SpecRevision::new(1),
            updated_at: Local::now(),
        }
    }

    pub fn next(&self, payload: TaskConfig) -> Self {
        Self {
            value: payload,
            revision: self.revision.next(),
            updated_at: Local::now(),
        }
    }

    pub fn name(&self) -> &str {
        &self.value.name()
    }

    pub fn query(&self) -> &Query {
        &self.value.query()
    }

    pub fn poll_config(&self) -> PollConfig {
        self.value.poll_config()
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
    pub fn try_new(
        name: String,
        query: Query,
        poll_config: PollConfig,
        deep_history: u8,
    ) -> Result<Self, TaskError> {
        if name.trim().is_empty() {
            return Err(TaskError::EmptyName);
        }

        Ok(Self {
            name,
            query,
            poll_config,
            deep_history,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn poll_config(&self) -> PollConfig {
        self.poll_config
    }

    pub fn deep_history(&self) -> u8 {
        self.deep_history
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{monitor::adapter::SnmpGetQuery, polling::AttemptConfig};
    use tokio::time::Duration;

    fn valid_poll_config() -> PollConfig {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        PollConfig::try_new(Duration::from_secs(1), 0, attempt).unwrap()
    }

    fn valid_query() -> Query {
        Query::SnmpGet(
            SnmpGetQuery::from_raw(
                "127.0.0.1".to_string(),
                161,
                "public".to_string(),
                None,
                vec![],
            )
            .unwrap(),
        )
    }

    #[test]
    fn rejects_empty_name() {
        let err = TaskConfig::try_new("   ".to_string(), valid_query(), valid_poll_config(), 3)
            .unwrap_err();
        assert!(matches!(err, TaskError::EmptyName));
    }

    #[test]
    fn accepts_valid_spec() {
        let spec =
            TaskConfig::try_new("T-1".to_string(), valid_query(), valid_poll_config(), 3).unwrap();
        assert_eq!(spec.name(), "T-1");
        assert_eq!(spec.deep_history(), 3);
    }
}
