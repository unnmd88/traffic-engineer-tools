use chrono::{DateTime, Local};

use crate::{
    monitor::task::{SpecRevision, UseCaseQuery},
    polling::PollConfig,
};

use super::error::TaskError;

/// Валидированное декларативное описание (Spec) — неизменяемый value object.
///
/// Создаётся только через [`TaskSpecPayload::try_new`] — невалидная спека невыразима
/// (поля приватные).
#[derive(Clone, Debug)]
pub struct TaskSpecPayload {
    name: String,
    query: UseCaseQuery,
    poll_config: PollConfig,
    deep_history: u8,
}

/// Спека + версия и время постановки. Payload неизменяем; обновление — замена целиком.
#[derive(Clone, Debug)]
pub struct TaskSpec {
    value: TaskSpecPayload,
    revision: SpecRevision,
    updated_at: DateTime<Local>,
}

impl TaskSpec {
    pub fn new(payload: TaskSpecPayload) -> Self {
        Self {
            value: payload,
            revision: SpecRevision::new(0),
            updated_at: Local::now(),
        }
    }

    pub fn next(&self, payload: TaskSpecPayload) -> Self {
        Self {
            value: payload,
            revision: self.revision.next(),
            updated_at: Local::now(),
        }
    }

    pub fn payload(&self) -> &TaskSpecPayload {
        &self.value
    }

    pub fn revision(&self) -> SpecRevision {
        self.revision
    }

    pub fn updated_at(&self) -> DateTime<Local> {
        self.updated_at
    }
}

impl TaskSpecPayload {
    pub fn try_new(
        name: String,
        query: UseCaseQuery,
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

    pub fn query(&self) -> &UseCaseQuery {
        &self.query
    }

    pub fn poll_config(&self) -> &PollConfig {
        &self.poll_config
    }

    pub fn deep_history(&self) -> u8 {
        self.deep_history
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{monitor::task::QuerySnmpGet, polling::AttemptConfig};
    use tokio::time::Duration;

    fn valid_poll_config() -> PollConfig {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        PollConfig::try_new(Duration::from_secs(1), 0, attempt).unwrap()
    }

    fn valid_query() -> UseCaseQuery {
        UseCaseQuery::SnmpGet(
            QuerySnmpGet::from_raw(
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
        let err =
            TaskSpecPayload::try_new("   ".to_string(), valid_query(), valid_poll_config(), 3)
                .unwrap_err();
        assert!(matches!(err, TaskError::EmptyName));
    }

    #[test]
    fn accepts_valid_spec() {
        let spec =
            TaskSpecPayload::try_new("T-1".to_string(), valid_query(), valid_poll_config(), 3)
                .unwrap();
        assert_eq!(spec.name(), "T-1");
        assert_eq!(spec.deep_history(), 3);
    }
}
