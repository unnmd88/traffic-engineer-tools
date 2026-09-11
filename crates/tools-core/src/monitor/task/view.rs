use chrono::{DateTime, Local};
use tokio::time::Duration;

use crate::{
    monitor::{
        task::{SpecRevision, TaskId, TaskStatus},
        usecase::UseCaseOutput,
    },
    polling::{Metrics, Response},
};

use super::entity::TaskEntity;

/// Неизменяемое представление одной задачи для интерфейса (read-model).
///
/// В отличие от `TaskEntity`, не содержит спеки целиком и не привязан к
/// внутреннему устройству репозитория. `Serialize` добавится, когда появится
/// JSON-транспорт (websocket / БД).
#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: TaskId,
    pub name: String,
    pub revision: SpecRevision,
    pub target: String,
    pub status: TaskStatus,
    pub interval: Duration,
    pub limit: u64,
    pub metrics: Metrics,
    pub result: Option<Response<UseCaseOutput>>,
    pub history: Vec<HistoryEntryView>,
}

/// Одна строка сжатой истории (для отображения).
#[derive(Debug, Clone)]
pub struct HistoryEntryView {
    pub timestamp: DateTime<Local>,
    pub result: Option<Response<UseCaseOutput>>,
}

/// Полный снапшот монитора: используется для первичной синхронизации
/// (`get_snapshot`) и как «база» для дельт (`OrchestratorEvent::TaskUpdated`).
#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub tasks: Vec<TaskView>,
}

impl From<&TaskEntity> for TaskView {
    fn from(t: &TaskEntity) -> Self {
        let history = t
            .history()
            .iter()
            .map(|h| HistoryEntryView {
                timestamp: h.timestamp,
                result: h.snapshot.poll_result().cloned(),
            })
            .collect();

        Self {
            id: *t.id(),
            revision: t.spec_revision(),
            name: t.name().to_string(),
            target: t.query().target(),
            status: *t.status(),
            interval: t.poll_config().interval(),
            limit: t.poll_config().limit(),
            metrics: *t.metrics(),
            result: t.snapshot().poll_result().cloned(),
            history,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::task::{QuerySnmpGet, TaskSpecPayload, UseCaseQuery},
        polling::{AttemptConfig, PollConfig},
    };
    use tokio::time::Duration;

    fn make_spec() -> TaskSpecPayload {
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 1, Duration::from_millis(10))
                .unwrap();
        let poll = PollConfig::try_new(Duration::from_secs(5), 100, attempt).unwrap();
        let query = UseCaseQuery::SnmpGet(
            QuerySnmpGet::from_raw(
                "127.0.0.1".to_string(),
                161,
                "public".to_string(),
                None,
                vec![],
            )
            .unwrap(),
        );
        TaskSpecPayload::try_new("T-1".to_string(), query, poll, 3).unwrap()
    }

    #[test]
    fn entity_converts_to_view() {
        let entity = TaskEntity::new(TaskId::new(1), make_spec());
        let view = TaskView::from(&entity);

        assert_eq!(view.name, "T-1");
        assert_eq!(view.target, "127.0.0.1:161");
        assert_eq!(view.status, TaskStatus::Idle);
        assert_eq!(view.interval, Duration::from_secs(5));
        assert_eq!(view.limit, 100);
        assert!(view.result.is_none());
        assert!(view.history.is_empty());
    }
}
