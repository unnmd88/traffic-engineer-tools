use std::time::Duration;

use tools_core::{
    monitor::{
        AppConfig, Application, ApplicationError, ApplicationHandle, ApplicationId, EventData,
        EventMeta, ExecutionMetrics, Generation, Health, HistoryEntryView, MonitorEvent,
        MonitorSnapshot, MonitorSubscription, OrchestratorError, PollSchedule, RestartPolicy,
        RunningApplication, ScenarioSpec, SnmpGetScenarioSpec, SnmpGetSpec, SnmpSetScenarioSpec,
        SnmpSetSpec, SpecRevision, TaskConfig, TaskId, TaskStatus, TaskView,
    },
    polling::AttemptConfig,
};

fn task(name: &str, set: bool) -> TaskConfig {
    let target = "127.0.0.1:1161".parse().unwrap();
    let community = "public".parse().unwrap();
    let schedule = PollSchedule::FixedDelay {
        delay: Duration::from_secs(1),
    };
    let attempt = AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap();
    let scenario = if set {
        let request = SnmpSetSpec::try_new(target, None, community, None, vec![]).unwrap();
        ScenarioSpec::SnmpSet(SnmpSetScenarioSpec::try_new(request, schedule, attempt).unwrap())
    } else {
        let request = SnmpGetSpec::new(target, community, None, vec![]);
        ScenarioSpec::SnmpGet(SnmpGetScenarioSpec::try_new(request, schedule, attempt).unwrap())
    };
    TaskConfig::try_new(name.to_string(), scenario, 0, 3).unwrap()
}

#[test]
fn prepared_application_exposes_configuration_and_snapshot_types() {
    let application = Application::new(
        AppConfig {
            restart: RestartPolicy::default(),
            history_sink: None,
        },
        vec![task("get", false), task("set", true)],
    );
    let id: &ApplicationId = application.id();
    let subscription: MonitorSubscription = application.subscribe_with_snapshot();
    let snapshot: MonitorSnapshot = subscription.snapshot;
    assert_eq!(snapshot.application_id, *id);
    assert_eq!(snapshot.tasks.len(), 2);

    for (index, view) in snapshot.tasks.iter().enumerate() {
        let view: &TaskView = view;
        let task_id: TaskId = view.id;
        let revision: SpecRevision = view.revision;
        let metrics: ExecutionMetrics = view.metrics;
        let health: &Health = &view.health;
        let history: &[HistoryEntryView] = &view.history;
        assert_eq!(task_id, application.task_ids()[index]);
        assert_eq!(revision, SpecRevision::new(1));
        assert_eq!(view.status, TaskStatus::Idle);
        assert_eq!(view.created_at, view.config_updated_at);
        assert_eq!(view.created_at, view.updated_at);
        assert_eq!(metrics.executions, 0);
        assert!(health.last_error.is_none());
        assert!(history.is_empty());
    }
    assert!(matches!(snapshot.tasks[0].scenario, ScenarioSpec::SnmpGet(_)));
    assert!(matches!(snapshot.tasks[1].scenario, ScenarioSpec::SnmpSet(_)));
}

#[tokio::test]
async fn handle_updates_configuration_and_publishes_public_events() {
    let running: RunningApplication = Application::new(AppConfig::default(), vec![]).spawn();
    let handle: ApplicationHandle = running.handle();
    let mut subscription = handle.subscribe_with_snapshot().await.unwrap();
    let id = handle.add_task(task("initial", false)).await.unwrap();
    let initial = handle.get_snapshot().await.unwrap().tasks.remove(0);
    let event: MonitorEvent = subscription.events.recv().await.unwrap();
    let metadata: EventMeta = event.metadata;
    assert_eq!(metadata.application_id, *handle.id());
    let generation: Generation = metadata.generation;
    assert_eq!(generation, Generation::first());
    assert_eq!(metadata.seq, subscription.snapshot.next_seq);
    assert_eq!(metadata.task_id, id);
    assert!(matches!(event.data, EventData::TaskAdded));

    handle.update_task(id, task("updated", true)).await.unwrap();
    let updated = handle.get_snapshot().await.unwrap().tasks.remove(0);
    let event = subscription.events.recv().await.unwrap();
    assert!(matches!(event.data, EventData::TaskSpecUpdated));
    assert_eq!(event.metadata.revision, initial.revision.next());
    assert_eq!(updated.revision, event.metadata.revision);
    assert_eq!(updated.name, "updated");
    assert!(matches!(updated.scenario, ScenarioSpec::SnmpSet(_)));
    assert_eq!(updated.created_at, initial.created_at);
    assert!(updated.config_updated_at >= initial.config_updated_at);
    assert_eq!(updated.updated_at, updated.config_updated_at);

    handle.remove_task(id).await.unwrap();
    let error: OrchestratorError = handle.remove_task(id).await.unwrap_err();
    assert!(matches!(error, OrchestratorError::TaskNotFound(missing) if missing == id));
    let error: ApplicationError = error.into();
    assert!(matches!(error, ApplicationError::Orchestrator(_)));
    running.shutdown().await.unwrap();
    assert!(matches!(handle.get_snapshot().await, Err(OrchestratorError::ChannelClosed)));
}
