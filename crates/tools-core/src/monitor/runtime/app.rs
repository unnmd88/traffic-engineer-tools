use std::sync::Arc;

use derive_more::Display;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::monitor::{
    event::MonitorEvent,
    runtime::{
        MonitorSubscription,
        error::{ApplicationError, OrchestratorError},
        orchestrator::{Orchestrator, OrchestratorHandle},
        restart_policy::RestartPolicy,
    },
    task::{MonitorSnapshot, TaskConfig, TaskId},
};

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Hash)]
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

#[must_use]
pub struct Application {
    orchestrator: Orchestrator,
    handle: ApplicationHandle,
    task_ids: Vec<TaskId>,
}

#[derive(Clone)]
pub struct ApplicationHandle {
    uid: ApplicationId,
    orchestrator: OrchestratorHandle,
}

#[must_use]
pub struct RunningApplication {
    handle: ApplicationHandle,
    join: JoinHandle<()>,
}

impl Application {
    pub fn new(config: AppConfig, tasks: Vec<TaskConfig>) -> Self {
        let uid = ApplicationId::generate();
        let (mut orchestrator, orchestrator_handle) =
            Orchestrator::new(config.history_sink, config.restart, uid);
        let task_ids = orchestrator.prepare_tasks(tasks);

        Self {
            orchestrator,
            handle: ApplicationHandle {
                uid,
                orchestrator: orchestrator_handle,
            },
            task_ids,
        }
    }

    pub const fn id(&self) -> &ApplicationId {
        self.handle.id()
    }

    pub fn task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.handle.subscribe()
    }

    pub fn subscribe_with_snapshot(&self) -> MonitorSubscription {
        MonitorSubscription {
            snapshot: self.orchestrator.snapshot(),
            events: self.subscribe(),
        }
    }

    pub fn spawn(self) -> RunningApplication {
        RunningApplication {
            handle: self.handle,
            join: tokio::spawn(self.orchestrator.run()),
        }
    }
}

impl RunningApplication {
    pub fn handle(&self) -> ApplicationHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ApplicationError> {
        let result = self.handle.orchestrator.shutdown().await;
        (&mut self.join)
            .await
            .map_err(|error| ApplicationError::Runtime(Arc::new(error)))?;
        result.map_err(ApplicationError::from)
    }
}

impl Drop for RunningApplication {
    fn drop(&mut self) {
        self.join.abort();
    }
}

impl ApplicationHandle {
    pub const fn id(&self) -> &ApplicationId {
        &self.uid
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
}

#[cfg(test)]
mod tests {
    use tokio::{
        sync::{broadcast, mpsc},
        time::{Duration, timeout},
    };

    use crate::{
        monitor::{
            event::EventData,
            runtime::{AppConfig, Application, error::OrchestratorError},
            scenario::{PollSchedule, ScenarioSpec, SnmpGetScenarioSpec},
            task::{TaskConfig, TaskStatus},
        },
        polling::AttemptConfig,
        snmp::SnmpGetSpec,
    };

    fn spec(name: &str) -> TaskConfig {
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::from_millis(0))
                .unwrap();
        let scenario = ScenarioSpec::SnmpGet(
            SnmpGetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(1),
                },
                attempt,
            )
            .unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, 0, 3).unwrap()
    }

    #[test]
    fn construction_prepares_tasks_and_history_without_a_tokio_runtime() {
        let (sender, mut history) = mpsc::channel(1);
        let application = Application::new(
            AppConfig {
                history_sink: Some(sender),
                ..AppConfig::default()
            },
            vec![spec("first"), spec("second")],
        );
        let mut subscription = application.subscribe_with_snapshot();
        assert_eq!(subscription.snapshot.application_id, *application.id());
        assert_eq!(subscription.snapshot.next_seq, 0);
        assert_eq!(subscription.snapshot.tasks.len(), 2);
        assert_eq!(subscription.snapshot.tasks[0].name, "first");
        assert_eq!(subscription.snapshot.tasks[1].name, "second");
        let ids: Vec<_> = subscription
            .snapshot
            .tasks
            .iter()
            .map(|task| task.id)
            .collect();
        assert_eq!(application.task_ids(), ids);
        assert_ne!(ids[0], ids[1]);
        assert!(
            subscription
                .snapshot
                .tasks
                .iter()
                .all(|task| task.status == TaskStatus::Idle)
        );
        assert!(matches!(
            subscription.events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert!(matches!(history.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        drop(application);
        assert!(matches!(
            subscription.events.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        assert!(matches!(history.try_recv(), Err(mpsc::error::TryRecvError::Disconnected)));
    }

    #[tokio::test]
    async fn subscription_before_spawn_observes_commands_from_cloned_handles() {
        let application = Application::new(AppConfig::default(), vec![]);
        let mut subscription = application.subscribe_with_snapshot();
        let running = application.spawn();
        let app = running.handle();
        let other = app.clone();

        let id = other.add_task(spec("first")).await.unwrap();
        let added = subscription.events.recv().await.unwrap();
        assert_eq!(added.metadata.seq, subscription.snapshot.next_seq);
        assert_eq!(added.metadata.application_id, subscription.snapshot.application_id);
        assert_eq!(added.metadata.task_id, id);
        assert!(matches!(added.data, EventData::TaskAdded));
        assert_eq!(app.get_snapshot().await.unwrap().tasks[0].id, id);

        app.remove_task(id).await.unwrap();
        assert!(other.get_snapshot().await.unwrap().tasks.is_empty());
        assert!(matches!(subscription.events.recv().await.unwrap().data, EventData::TaskRemoved));
        running.shutdown().await.unwrap();

        assert!(matches!(
            subscription.events.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(matches!(other.get_snapshot().await, Err(OrchestratorError::ChannelClosed)));
    }

    #[tokio::test]
    async fn dropping_running_application_closes_events_and_history_with_handles_alive() {
        let (sender, mut history) = mpsc::channel(1);
        let application = Application::new(
            AppConfig {
                history_sink: Some(sender),
                ..AppConfig::default()
            },
            vec![],
        );
        let mut events = application.subscribe();
        let running = application.spawn();
        let app = running.handle();
        app.get_snapshot().await.unwrap();

        drop(running);
        timeout(Duration::from_secs(1), async {
            assert!(matches!(events.recv().await, Err(broadcast::error::RecvError::Closed)));
            assert!(history.recv().await.is_none());
            assert!(matches!(app.get_snapshot().await, Err(OrchestratorError::ChannelClosed)));
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_buffered_history_to_reach_the_external_channel() {
        let (sender, mut history) = mpsc::channel(1);
        let running = Application::new(
            AppConfig {
                history_sink: Some(sender),
                ..AppConfig::default()
            },
            vec![],
        )
        .spawn();
        let app = running.handle();
        let mut ids = Vec::new();
        for name in ["first", "second", "third"] {
            ids.push(app.add_task(spec(name)).await.unwrap());
        }

        let mut shutdown = Box::pin(running.shutdown());
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());
        tokio::task::yield_now().await;
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());

        let recorded = timeout(Duration::from_secs(1), async {
            let (result, recorded) = tokio::join!(shutdown, async {
                let mut recorded = Vec::new();
                while let Some(event) = history.recv().await {
                    recorded.push(event);
                }
                recorded
            });
            result.unwrap();
            recorded
        })
        .await
        .unwrap();
        assert_eq!(recorded.len(), ids.len());
        for ((seq, id), event) in ids.into_iter().enumerate().zip(recorded) {
            assert_eq!(event.metadata.seq, seq as u64);
            assert_eq!(event.metadata.task_id, id);
            assert!(matches!(event.data, EventData::TaskAdded));
        }
    }

    #[tokio::test]
    async fn cancelling_shutdown_aborts_the_runtime_and_history_writer() {
        let (sender, mut history) = mpsc::channel(1);
        let running = Application::new(
            AppConfig {
                history_sink: Some(sender),
                ..AppConfig::default()
            },
            vec![],
        )
        .spawn();
        let app = running.handle();
        app.add_task(spec("first")).await.unwrap();
        app.add_task(spec("second")).await.unwrap();
        let mut events = app.subscribe();
        let mut shutdown = Box::pin(running.shutdown());
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());
        tokio::task::yield_now().await;
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());

        drop(shutdown);
        timeout(Duration::from_secs(1), async {
            assert!(matches!(events.recv().await, Err(broadcast::error::RecvError::Closed)));
            while history.recv().await.is_some() {}
            assert!(matches!(app.get_snapshot().await, Err(OrchestratorError::ChannelClosed)));
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn subscription_returns_all_registered_tasks_before_the_next_event() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        let first = app.add_task(spec("first")).await.unwrap();
        let second = app.add_task(spec("second")).await.unwrap();
        let mut subscription = app.subscribe_with_snapshot().await.unwrap();
        assert_eq!(subscription.snapshot.application_id, *app.id());
        assert_eq!(subscription.snapshot.next_seq, 2);
        assert_eq!(subscription.snapshot.tasks.len(), 2);
        assert_eq!(subscription.snapshot.tasks[0].id, first);
        assert_eq!(subscription.snapshot.tasks[1].id, second);
        assert!(subscription.events.try_recv().is_err());

        app.remove_task(first).await.unwrap();
        let event = subscription.events.recv().await.unwrap();
        assert_eq!(event.metadata.seq, subscription.snapshot.next_seq);
        assert_eq!(event.metadata.task_id, first);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn add_then_snapshot_then_remove() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        let id = app.add_task(spec("t-1")).await.unwrap();

        let snap = app.get_snapshot().await.unwrap();
        assert_eq!(snap.tasks.len(), 1);
        assert_eq!(snap.tasks[0].id, id);
        assert_eq!(snap.tasks[0].status, TaskStatus::Idle);

        app.remove_task(id).await.unwrap();
        let snap = app.get_snapshot().await.unwrap();
        assert!(snap.tasks.is_empty());
        running.shutdown().await.unwrap();
    }
}
