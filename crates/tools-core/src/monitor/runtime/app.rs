use derive_more::Display;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::monitor::{
    event::MonitorEvent,
    runtime::{
        error::SupervisorError,
        supervisor::{Supervisor, SupervisorHandle},
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

/// Собранный рантайм: супервизор (владелец состояния и оба канала) + воркеры.
///
/// Тонкий фасад: команды, снапшот и подписка — всё через супервизор.
pub struct Application {
    uid: ApplicationId,
    supervisor: SupervisorHandle,
}

impl Application {
    pub fn new(hist_tx: Option<mpsc::Sender<MonitorEvent>>) -> Self {
        let (supervisor, supervisor_handle) = Supervisor::new(hist_tx);
        tokio::spawn(supervisor.run());

        Self {
            uid: ApplicationId::generate(),
            supervisor: supervisor_handle,
        }
    }

    pub fn id(&self) -> &ApplicationId {
        &self.uid
    }

    pub async fn start(&self, specs: Vec<TaskConfig>) -> Result<(), SupervisorError> {
        for spec in specs {
            let task_id = self.add_task(spec).await?;
            self.start_task(task_id).await?;
        }

        Ok(())
    }

    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, SupervisorError> {
        self.supervisor.add_task(spec).await
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        self.supervisor.remove_task(task_id).await
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        self.supervisor.start_task(task_id).await
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        self.supervisor.pause_task(task_id).await
    }

    pub async fn resume_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        self.supervisor.resume_task(task_id).await
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        self.supervisor.stop_task(task_id).await
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), SupervisorError> {
        self.supervisor.update_task(task_id, spec).await
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, SupervisorError> {
        self.supervisor.get_snapshot().await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.supervisor.subscribe()
    }

    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, sleep};

    use crate::{
        monitor::{
            adapter::{Query, SnmpGetQuery},
            runtime::Application,
            task::{MonitorSnapshot, TaskConfig, TaskStatus},
        },
        polling::{AttemptConfig, PollConfig},
    };

    fn spec(name: &str) -> TaskConfig {
        let query = Query::SnmpGet(
            SnmpGetQuery::from_raw(
                "127.0.0.1".to_string(),
                161,
                "public".to_string(),
                None,
                vec![],
            )
            .unwrap(),
        );
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let poll = PollConfig::try_new(Duration::from_secs(1), 0, attempt).unwrap();
        TaskConfig::try_new(name.to_string(), query, poll, 3).unwrap()
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
    async fn add_then_snapshot_then_remove() {
        let app = Application::new(None);
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
