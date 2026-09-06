use crate::{
    monitor::task::UseCaseQuery,
    polling::PollConfig,
};

use super::error::TaskError;

/// Спека задачи — валидированное декларативное описание (Spec).
///
/// Создаётся только через [`TaskSpec::try_new`] — невалидная спека невыразима
/// (поля приватные).
#[derive(Clone, Debug)]
pub struct TaskSpec {
    name: String,
    query: UseCaseQuery,
    poll_config: PollConfig,
    deep_history: u8,
}

impl TaskSpec {
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
    use crate::{
        monitor::task::QuerySnmpGet,
        polling::AttemptConfig,
    };
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
        let err = TaskSpec::try_new("   ".to_string(), valid_query(), valid_poll_config(), 3)
            .unwrap_err();
        assert!(matches!(err, TaskError::EmptyName));
    }

    #[test]
    fn accepts_valid_spec() {
        let spec = TaskSpec::try_new("T-1".to_string(), valid_query(), valid_poll_config(), 3)
            .unwrap();
        assert_eq!(spec.name(), "T-1");
        assert_eq!(spec.deep_history(), 3);
    }
}
