use std::time::Duration;

use tools_core::{
    monitor::{
        AppConfig, Application, ApplicationError, ApplicationHandle, ApplicationId, EventData,
        EventMeta, ExecutionMetrics, Generation, Health, HistoryEntryView, MonitorCommandError,
        MonitorEvent, MonitorSnapshot, MonitorSubscription, PollSchedule, RestartPolicy,
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
    assert!(matches!(event.data, EventData::TaskRegistered { .. }));

    handle.update_task(id, task("updated", true)).await.unwrap();
    let updated = handle.get_snapshot().await.unwrap().tasks.remove(0);
    let event = subscription.events.recv().await.unwrap();
    assert!(matches!(event.data, EventData::TaskConfigurationChanged { .. }));
    assert_eq!(event.metadata.revision, initial.revision.next());
    assert_eq!(updated.revision, event.metadata.revision);
    assert_eq!(updated.name, "updated");
    assert!(matches!(updated.scenario, ScenarioSpec::SnmpSet(_)));
    assert_eq!(updated.created_at, initial.created_at);
    assert!(updated.config_updated_at >= initial.config_updated_at);
    assert_eq!(updated.updated_at, updated.config_updated_at);

    handle.remove_task(id).await.unwrap();
    let error: MonitorCommandError = handle.remove_task(id).await.unwrap_err();
    assert!(matches!(error, MonitorCommandError::TaskNotFound(missing) if missing == id));
    let error: ApplicationError = error.into();
    assert!(matches!(error, ApplicationError::Command(_)));
    running.shutdown().await.unwrap();
    assert!(matches!(handle.get_snapshot().await, Err(MonitorCommandError::ChannelClosed)));
}

#[tokio::test]
async fn preparation_is_observable_before_scn_returns_and_survives_in_history() {
    use async_snmp::{Value, message::CommunityMessage};
    use tools_core::{
        monitor::{
            PreparationEvent, PreparationStage, PreparationStageOutcome, ScenarioBuildError,
        },
        polling::PollOutcome,
        snmp::{RawSnmpOidItem, SnmpError, SnmpValue},
    };

    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = SnmpGetSpec::from_raw(
        "127.0.0.1".to_string(),
        peer.local_addr().unwrap().port(),
        "public".to_string(),
        Some("potok_ug405".to_string()),
        vec![RawSnmpOidItem {
            name: None,
            oid: "stage".to_string(),
        }],
    )
    .unwrap();
    let attempt =
        AttemptConfig::try_new(Duration::from_secs(1), 3, Duration::from_millis(100)).unwrap();
    let scenario = ScenarioSpec::snmp_get(
        request,
        PollSchedule::FixedDelay {
            delay: Duration::from_secs(1),
        },
        attempt,
    )
    .unwrap();
    let config = TaskConfig::try_new("prepare".to_string(), scenario, 1, 3).unwrap();
    let (history, mut recorded) = tokio::sync::mpsc::channel(64);
    let application = Application::new(
        AppConfig {
            history_sink: Some(history),
            ..AppConfig::default()
        },
        vec![config],
    );
    let mut events = application.subscribe();
    let running = application.spawn();
    let handle = running.handle();
    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let fetching = matches!(&event.data, EventData::Preparation { event, .. }
            if matches!(event.as_ref(), PreparationEvent::StageStarted { stage: PreparationStage::FetchScn, .. }));
        observed.push(serde_json::to_value(event).unwrap());
        if fetching {
            break;
        }
    }
    let view = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(view.status, TaskStatus::Starting);
    assert_eq!(
        view.preparation.as_ref().unwrap().active_stage,
        Some(PreparationStage::FetchScn)
    );
    assert!(view.preparation.as_ref().unwrap().report.is_none());
    assert_eq!(view.metrics.executions, 0);

    let mut packet = [0; 2048];
    let (size, source) = tokio::time::timeout(Duration::from_secs(5), peer.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    let message = CommunityMessage::decode(packet[..size].to_vec().into()).unwrap();
    let mut response = message.pdu.standard().unwrap().to_response();
    response.varbinds[0].value = Value::Integer(42);
    let bytes = CommunityMessage::new(message.version, message.community, response).encode();
    peer.send_to(&bytes, source).await.unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let failed = matches!(event.data, EventData::BuildFailed { .. });
        assert!(!matches!(event.data, EventData::TaskStarted));
        observed.push(serde_json::to_value(event).unwrap());
        if failed {
            break;
        }
    }
    let view = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(view.status, TaskStatus::Restarting);
    assert_eq!(view.metrics.executions, 0);
    let preparation = view.preparation.unwrap();
    assert_eq!(preparation.revision, view.revision);
    assert!(preparation.active_stage.is_none());
    let report = preparation.report.unwrap();
    assert_eq!(report.stages.len(), 2);
    assert!(matches!(
        report.outcome,
        Err(ScenarioBuildError::Snmp(SnmpError::UnexpectedValueType { .. }))
    ));
    assert!(matches!(report.stages[1].outcome, PreparationStageOutcome::Failed(_)));
    let request = report.stages[1].request.as_ref().unwrap();
    assert_eq!(request.config.retries(), 0);
    assert_eq!(request.attempt_count(), 1);
    assert!(
        matches!(&request.outcome, PollOutcome::Succeeded(varbind) if matches!(varbind.value, SnmpValue::Integer(42)))
    );

    running.shutdown().await.unwrap();
    let mut history = Vec::new();
    while let Some(event) = recorded.recv().await {
        let json = serde_json::to_value(event).unwrap();
        let restored: MonitorEvent = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), json);
        history.push(json);
    }
    assert_eq!(history, observed);
}
