use derive_more::Display;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::monitor::{
    event::MonitorEvent,
    runtime::{
        MonitorSubscription,
        error::OrchestratorError,
        orchestrator::{Orchestrator, OrchestratorHandle},
        restart_policy::RestartPolicy,
    },
    task::{MonitorSnapshot, TaskConfig, TaskId},
};

#[derive(Clone, Display)]
pub struct ApplicationId(Uuid);

impl ApplicationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Default)]
pub struct AppConfig {
    pub history_sink: Option<mpsc::Sender<MonitorEvent>>,
    pub restart: RestartPolicy,
}

/// Собранный рантайм: оркестратор (владелец состояния и оба канала) + воркеры.
///
/// Тонкий фасад: команды, снапшот и подписка — всё через оркестратор.
pub struct Application {
    uid: ApplicationId,
    orchestrator: OrchestratorHandle,
}

impl Application {
    pub fn new(config: AppConfig) -> Self {
        let uid = ApplicationId::generate();
        let (orchestrator, orchestrator_handle) =
            Orchestrator::new(config.history_sink, config.restart, uid.to_string());
        tokio::spawn(orchestrator.run());

        Self {
            uid,
            orchestrator: orchestrator_handle,
        }
    }

    pub fn id(&self) -> &ApplicationId {
        &self.uid
    }

    pub async fn start(&self, specs: Vec<TaskConfig>) -> Result<(), OrchestratorError> {
        for spec in specs {
            let task_id = self.add_task(spec).await?;
            self.start_task(task_id).await?;
        }

        Ok(())
    }

    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, OrchestratorError> {
        self.orchestrator.add_task(spec).await
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.orchestrator.remove_task(task_id).await
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.orchestrator.start_task(task_id).await
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.orchestrator.pause_task(task_id).await
    }

    pub async fn resume_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.orchestrator.resume_task(task_id).await
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.orchestrator.stop_task(task_id).await
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), OrchestratorError> {
        self.orchestrator.update_task(task_id, spec).await
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, OrchestratorError> {
        self.orchestrator.get_snapshot().await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.orchestrator.subscribe()
    }

    pub async fn subscribe_with_snapshot(&self) -> Result<MonitorSubscription, OrchestratorError> {
        self.orchestrator.subscribe_with_snapshot().await
    }

    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        self.orchestrator.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, sleep};

    use crate::{
        monitor::{
            probe::SnmpGetSpec,
            runtime::{AppConfig, Application},
            scenario::{Scenario, SnmpGetScenarioSpec},
            task::{MonitorSnapshot, TaskConfig, TaskStatus},
        },
        polling::AttemptConfig,
    };

    fn spec(name: &str) -> TaskConfig {
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let scenario = Scenario::SnmpGet(
            SnmpGetScenarioSpec::try_new(request, Duration::from_secs(1), attempt).unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, 0, 3).unwrap()
    }

    async fn poll_until(
        app: &Application,
        mut pred: impl FnMut(&MonitorSnapshot) -> bool,
    ) -> MonitorSnapshot {
        for _ in 0..200 {
            let snap = app.get_snapshot().await.expect("snapshot");
            if pred(&snap) {
                return snap;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("snapshot condition not met in time");
    }

    #[tokio::test]
    async fn subscription_returns_all_registered_tasks_before_the_next_event() {
        let app = Application::new(AppConfig::default());
        let first = app.add_task(spec("first")).await.unwrap();
        let second = app.add_task(spec("second")).await.unwrap();
        let mut subscription = app.subscribe_with_snapshot().await.unwrap();
        assert_eq!(subscription.snapshot.source, app.id().to_string());
        assert_eq!(subscription.snapshot.next_seq, 2);
        assert_eq!(subscription.snapshot.tasks.len(), 2);
        assert_eq!(subscription.snapshot.tasks[0].id, first);
        assert_eq!(subscription.snapshot.tasks[1].id, second);
        assert!(subscription.events.try_recv().is_err());

        app.remove_task(first).await.unwrap();
        let event = subscription.events.recv().await.unwrap();
        assert_eq!(event.metadata.seq, subscription.snapshot.next_seq);
        assert_eq!(event.metadata.task_id, first);
        app.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn add_then_snapshot_then_remove() {
        let app = Application::new(AppConfig::default());
        let id = app.add_task(spec("t-1")).await.unwrap();

        // задача появляется в снапшоте (eventually-consistent: Added уходит асинхронно)
        let snap = poll_until(&app, |s| !s.tasks.is_empty()).await;
        assert_eq!(snap.tasks.len(), 1);
        assert_eq!(snap.tasks[0].id, id);
        assert_eq!(snap.tasks[0].status, TaskStatus::Idle);

        app.remove_task(id).await.unwrap();
        let snap = poll_until(&app, |s| s.tasks.is_empty()).await;
        assert!(snap.tasks.is_empty());
    }
}
