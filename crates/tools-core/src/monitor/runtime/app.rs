use serde::{Deserialize, Serialize};

use std::sync::Arc;

use derive_more::Display;
use tokio::{sync::broadcast, task::JoinHandle};
use uuid::Uuid;

use crate::monitor::{
    event::MonitorEvent,
    runtime::{
        MonitorSubscription,
        command::OrchestratorHandle,
        error::{ApplicationError, MonitorCommandError},
        orchestrator::Orchestrator,
        ordered::OrderedOrchestrator,
        restart_policy::RestartPolicy,
    },
    snapshot::MonitorSnapshot,
    task::{RunLimit, TaskConfig, TaskId},
};

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApplicationId(Uuid);

impl ApplicationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Default)]
pub struct AppConfig {
    pub restart: RestartPolicy,
}

/// A task to register and start automatically when the monitor is spawned.
#[derive(Clone, Debug)]
pub struct InitialTask {
    /// The task to register before the monitor starts.
    pub config: TaskConfig,
    /// The limit to use for the automatic start.
    pub limit: RunLimit,
}

/// A prepared monitor, before it is started.
///
/// `Application::new` is synchronous, assigns ids to the initial tasks, and does
/// not touch the network. Subscribe before [`Application::spawn`] to receive startup
/// events through the bounded event stream.
///
/// # Example
///
/// ```
/// use tools_core::monitor::{AppConfig, Application};
///
/// let app = Application::new(AppConfig::default(), vec![]);
/// let sub = app.subscribe_with_snapshot();
/// assert_eq!(sub.snapshot.tasks.len(), 0);
/// ```
#[must_use]
pub struct Application {
    orchestrator: Prepared,
    handle: ApplicationHandle,
    task_ids: Vec<TaskId>,
}

enum Prepared {
    Current(Box<Orchestrator>),
    Ordered(Box<OrderedOrchestrator>),
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
    pub fn new(config: AppConfig, tasks: Vec<InitialTask>) -> Self {
        let uid = ApplicationId::generate();
        let (mut orchestrator, orchestrator_handle) = Orchestrator::new(config.restart, uid);
        let task_ids = orchestrator.prepare_tasks(tasks);

        Self {
            orchestrator: Prepared::Current(Box::new(orchestrator)),
            handle: ApplicationHandle {
                uid,
                orchestrator: orchestrator_handle,
            },
            task_ids,
        }
    }

    /// Prepares a monitor using the alternative orchestrator with an ordered command and report queue.
    /// Its stop and removal replies confirm local exit and published results from the retiring run.
    pub fn new_ordered(config: AppConfig, tasks: Vec<InitialTask>) -> Self {
        let uid = ApplicationId::generate();
        let (orchestrator, handle, task_ids) = OrderedOrchestrator::new(uid, config, tasks);
        Self {
            orchestrator: Prepared::Ordered(Box::new(orchestrator)),
            handle: ApplicationHandle {
                uid,
                orchestrator: handle,
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
            snapshot: match &self.orchestrator {
                Prepared::Current(actor) => actor.snapshot(),
                Prepared::Ordered(actor) => actor.snapshot(),
            },
            events: self.subscribe(),
        }
    }

    pub fn spawn(self) -> RunningApplication {
        RunningApplication {
            handle: self.handle,
            join: match self.orchestrator {
                Prepared::Current(actor) => tokio::spawn(actor.run()),
                Prepared::Ordered(actor) => tokio::spawn(actor.run()),
            },
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

    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, MonitorCommandError> {
        self.orchestrator.add_task(spec).await
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        self.orchestrator.remove_task(task_id).await
    }

    /// Starts an idle or terminal task with a new run limit and a fresh count.
    ///
    /// The reply confirms that a start was requested; preparation runs in the background.
    /// Repeating a start with the same limit leaves an ongoing execution in place.
    /// A different limit returns [`MonitorCommandError::TaskAlreadyRunning`].
    pub async fn start_task(
        &self,
        task_id: TaskId,
        limit: RunLimit,
    ) -> Result<(), MonitorCommandError> {
        self.orchestrator.start_task(task_id, limit).await
    }

    /// Requests pause. A live scenario applies it between operations and reports
    /// the transition to `Paused`. A later resume request can supersede it.
    pub async fn pause_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        self.orchestrator.pause_task(task_id).await
    }

    /// Requests resumption, including when a live scenario has yet to apply
    /// its requested pause. A live scenario reports the transition to `Active`
    /// when resumption changes its applied mode.
    pub async fn resume_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        self.orchestrator.resume_task(task_id).await
    }

    /// Stops the current execution and waits for its local scenario to finish.
    ///
    /// Pending automatic restarts are invalidated. An already-sent device request
    /// may still take effect. An idle or terminal task keeps its current status.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorCommandError::TaskNotFound`] for an unknown task or
    /// [`MonitorCommandError::ChannelClosed`] if the monitor closes before replying.
    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        self.orchestrator.stop_task(task_id).await
    }

    /// Replaces the configuration of an idle, stopped, or completed task.
    ///
    /// The new revision is used at the next start. Previous results keep their
    /// original revision and history is retained up to the new configured depth.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorCommandError::TaskUpdateWhileRunning`] during preparation,
    /// active execution, pause, or automatic restart. An unknown task returns
    /// [`MonitorCommandError::TaskNotFound`]. If the monitor closes before replying,
    /// returns [`MonitorCommandError::ChannelClosed`].
    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), MonitorCommandError> {
        self.orchestrator.update_task(task_id, spec).await
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, MonitorCommandError> {
        self.orchestrator.get_snapshot().await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.orchestrator.subscribe()
    }

    pub async fn subscribe_with_snapshot(
        &self,
    ) -> Result<MonitorSubscription, MonitorCommandError> {
        self.orchestrator.subscribe_with_snapshot().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        sync::broadcast,
        time::{Duration, timeout},
    };

    use crate::{
        monitor::{
            event::EventData,
            runtime::{AppConfig, Application, InitialTask, error::MonitorCommandError},
            scenario::{PollSchedule, ScenarioSpec, SnmpGetScenarioSpec},
            task::{RunLimit, TaskConfig, TaskStatus},
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
        TaskConfig::try_new(name.to_string(), scenario, 3).unwrap()
    }

    #[test]
    fn construction_prepares_tasks_without_a_tokio_runtime() {
        let application = Application::new(
            AppConfig::default(),
            ["first", "second"]
                .into_iter()
                .map(|name| InitialTask {
                    config: spec(name),
                    limit: RunLimit::Unlimited,
                })
                .collect(),
        );
        let mut subscription = application.subscribe_with_snapshot();
        assert_eq!(subscription.snapshot.application_id, *application.id());
        assert_eq!(subscription.snapshot.next_seq, 0);
        assert_eq!(subscription.snapshot.tasks.len(), 2);
        assert_eq!(
            subscription.snapshot.tasks[0]
                .configuration
                .description
                .name,
            "first"
        );
        assert_eq!(
            subscription.snapshot.tasks[1]
                .configuration
                .description
                .name,
            "second"
        );
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

        drop(application);
        assert!(matches!(
            subscription.events.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
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
        assert!(matches!(added.data, EventData::TaskRegistered(_)));
        assert_eq!(app.get_snapshot().await.unwrap().tasks[0].id, id);

        app.remove_task(id).await.unwrap();
        assert!(other.get_snapshot().await.unwrap().tasks.is_empty());
        assert!(matches!(subscription.events.recv().await.unwrap().data, EventData::TaskRemoved));
        running.shutdown().await.unwrap();

        assert!(matches!(
            subscription.events.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(matches!(other.get_snapshot().await, Err(MonitorCommandError::ChannelClosed)));
    }

    #[tokio::test]
    async fn dropping_running_application_closes_events_with_handles_alive() {
        for prepare in [Application::new, Application::new_ordered] {
            let application = prepare(AppConfig::default(), vec![]);
            let mut events = application.subscribe();
            let running = application.spawn();
            let app = running.handle();
            app.get_snapshot().await.unwrap();

            drop(running);
            timeout(Duration::from_secs(1), async {
                assert!(matches!(events.recv().await, Err(broadcast::error::RecvError::Closed)));
                assert!(matches!(
                    app.get_snapshot().await,
                    Err(MonitorCommandError::ChannelClosed)
                ));
            })
            .await
            .unwrap();
        }
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
