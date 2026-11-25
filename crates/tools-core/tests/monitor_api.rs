use std::{num::NonZeroU64, time::Duration};

use tools_core::{
    monitor::{
        AppConfig, Application, ApplicationError, ApplicationHandle, ApplicationId, EventData,
        EventMeta, ExecutionMetrics, Generation, InitialTask, MonitorCommandError, MonitorEvent,
        MonitorSnapshot, MonitorSubscription, PollSchedule, RestartPolicy, RunLimit,
        RunningApplication, ScenarioDescription, ScenarioExecution, ScenarioSpec,
        SnmpGetScenarioSpec, SnmpGetSpec, SnmpSetScenarioSpec, SnmpSetSpec, SpecRevision,
        TaskConfig, TaskConfigurationSnapshot, TaskId, TaskSnapshot, TaskStatus,
    },
    polling::AttemptConfig,
};

type Prepare = fn(AppConfig, Vec<InitialTask>) -> Application;

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
    TaskConfig::try_new(name.to_string(), scenario, 3).unwrap()
}

#[test]
fn prepared_application_exposes_configuration_and_snapshot_types() {
    check_prepared_application_exposes_configuration_and_snapshot_types(Application::new);
}

#[test]
fn ordered_prepared_application_exposes_configuration_and_snapshot_types() {
    check_prepared_application_exposes_configuration_and_snapshot_types(Application::new_ordered);
}

fn check_prepared_application_exposes_configuration_and_snapshot_types(prepare: Prepare) {
    let application = prepare(
        AppConfig {
            restart: RestartPolicy::default(),
        },
        vec![
            InitialTask {
                config: task("get", false),
                limit: RunLimit::Unlimited,
            },
            InitialTask {
                config: task("set", true),
                limit: RunLimit::Runs(NonZeroU64::MIN),
            },
        ],
    );
    let id: &ApplicationId = application.id();
    let subscription: MonitorSubscription = application.subscribe_with_snapshot();
    let snapshot: MonitorSnapshot = subscription.snapshot;
    assert_eq!(snapshot.application_id, *id);
    assert_eq!(snapshot.tasks.len(), 2);

    for (index, view) in snapshot.tasks.iter().enumerate() {
        let view: &TaskSnapshot = view;
        let configuration: &TaskConfigurationSnapshot = &view.configuration;
        let task_id: TaskId = view.id;
        let revision: SpecRevision = configuration.revision;
        let metrics: ExecutionMetrics = view.metrics;
        let history: &[ScenarioExecution] = &view.history;
        assert_eq!(task_id, application.task_ids()[index]);
        assert_eq!(revision, SpecRevision::new(1));
        assert_eq!(view.status, TaskStatus::Idle);
        assert_eq!(view.run_limit, None);
        assert_eq!(view.completed_runs, 0);
        assert!(view.progress.is_none());
        assert_eq!(view.created_at, configuration.accepted_at);
        assert_eq!(view.created_at, view.updated_at);
        assert_eq!(metrics.executions, 0);
        assert!(view.last_failure.is_none());
        assert_eq!(metrics.consecutive_failures, 0);
        assert!(history.is_empty());
    }
    assert!(matches!(
        snapshot.tasks[0].configuration.description.scenario,
        ScenarioDescription::SnmpGet { .. }
    ));
    assert!(matches!(
        snapshot.tasks[1].configuration.description.scenario,
        ScenarioDescription::SnmpSet { .. }
    ));
}

#[tokio::test]
async fn handle_updates_configuration_and_publishes_public_events() {
    check_handle_updates_configuration_and_publishes_public_events(Application::new).await;
}

#[tokio::test]
async fn ordered_handle_updates_configuration_and_publishes_public_events() {
    check_handle_updates_configuration_and_publishes_public_events(Application::new_ordered).await;
}

async fn check_handle_updates_configuration_and_publishes_public_events(prepare: Prepare) {
    let running: RunningApplication = prepare(AppConfig::default(), vec![]).spawn();
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
    assert!(matches!(event.data, EventData::TaskRegistered(_)));

    handle.update_task(id, task("updated", true)).await.unwrap();
    let updated = handle.get_snapshot().await.unwrap().tasks.remove(0);
    let event = subscription.events.recv().await.unwrap();
    assert!(matches!(event.data, EventData::TaskConfigurationChanged(_)));
    assert_eq!(event.metadata.revision, initial.configuration.revision.next());
    assert_eq!(updated.configuration.revision, event.metadata.revision);
    assert_eq!(updated.configuration.description.name, "updated");
    assert!(matches!(
        updated.configuration.description.scenario,
        ScenarioDescription::SnmpSet { .. }
    ));
    assert_eq!(updated.created_at, initial.created_at);
    assert!(updated.configuration.accepted_at >= initial.configuration.accepted_at);
    assert_eq!(updated.updated_at, updated.configuration.accepted_at);

    handle.start_task(id, RunLimit::Unlimited).await.unwrap();
    assert!(matches!(
        handle.update_task(id, task("too early", false)).await,
        Err(MonitorCommandError::TaskUpdateWhileRunning { task_id, .. }) if task_id == id
    ));
    handle.stop_task(id).await.unwrap();
    handle
        .update_task(id, task("next run", false))
        .await
        .unwrap();
    let stopped = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(stopped.id, id);
    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert_eq!(stopped.configuration.revision, updated.configuration.revision.next());

    handle.remove_task(id).await.unwrap();
    let error: MonitorCommandError = handle.remove_task(id).await.unwrap_err();
    assert!(matches!(error, MonitorCommandError::TaskNotFound(missing) if missing == id));
    let error: ApplicationError = error.into();
    assert!(matches!(error, ApplicationError::Command(_)));
    running.shutdown().await.unwrap();
    assert!(matches!(handle.get_snapshot().await, Err(MonitorCommandError::ChannelClosed)));
}

#[tokio::test(start_paused = true)]
async fn one_registered_task_can_start_again_with_a_different_limit() {
    check_one_registered_task_can_start_again_with_a_different_limit(Application::new).await;
}

#[tokio::test(start_paused = true)]
async fn ordered_one_registered_task_can_start_again_with_a_different_limit() {
    check_one_registered_task_can_start_again_with_a_different_limit(Application::new_ordered)
        .await;
}

async fn check_one_registered_task_can_start_again_with_a_different_limit(prepare: Prepare) {
    use async_snmp::{Value, message::CommunityMessage};
    use tokio::{net::UdpSocket, time::timeout};
    use tools_core::{monitor::TaskTransitionCause, snmp::RawSnmpOidItem};

    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = SnmpGetSpec::from_raw(
        "127.0.0.1".to_string(),
        peer.local_addr().unwrap().port(),
        "public".to_string(),
        None,
        vec![RawSnmpOidItem {
            name: None,
            oid: "1.3.6.1.2.1.1.1.0".to_string(),
        }],
    )
    .unwrap();
    let scenario = ScenarioSpec::snmp_get(
        request,
        PollSchedule::fixed_delay(Duration::from_secs(1)).unwrap(),
        AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap(),
    )
    .unwrap();
    let running = prepare(AppConfig::default(), vec![]).spawn();
    let handle = running.handle();
    let id = handle
        .add_task(TaskConfig::try_new("get".to_string(), scenario, 3).unwrap())
        .await
        .unwrap();
    let mut events = handle.subscribe();

    for count in [1, 2] {
        let limit = RunLimit::Runs(NonZeroU64::new(count).unwrap());
        handle.start_task(id, limit).await.unwrap();
        assert!(matches!(
            handle.start_task(id, RunLimit::Unlimited).await,
            Err(MonitorCommandError::TaskAlreadyRunning(running_id)) if running_id == id
        ));
        let event = events.recv().await.unwrap();
        assert!(matches!(event.data, EventData::TaskStateChanged(transition)
            if transition.current == TaskStatus::Preparing && transition.run_limit == Some(limit)));
        let view = handle.get_snapshot().await.unwrap().tasks.remove(0);
        assert_eq!(view.run_limit, Some(limit));
        assert_eq!(view.completed_runs, 0);

        for _ in 0..count {
            let mut bytes = [0; 2048];
            let (len, source) = timeout(Duration::from_secs(10), peer.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            let request = CommunityMessage::decode(bytes[..len].to_vec().into()).unwrap();
            let mut response = request.pdu.standard().unwrap().to_response();
            response.varbinds[0].value = Value::OctetString(b"test device".to_vec().into());
            let message =
                CommunityMessage::new(request.version, request.community, response).encode();
            peer.send_to(&message, source).await.unwrap();
        }

        timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.unwrap();
                if let EventData::TaskStateChanged(transition) = event.data
                    && transition.current == TaskStatus::Completed
                {
                    assert_eq!(event.metadata.task_id, id);
                    assert!(matches!(transition.cause, TaskTransitionCause::LimitReached { limit, .. } if limit == count));
                    break;
                }
            }
        }).await.unwrap();
        let completed = handle.get_snapshot().await.unwrap();
        assert_eq!(completed.tasks[0].completed_runs, count);
        assert_eq!(completed.tasks[0].configuration.revision, SpecRevision::new(1));
        handle.stop_task(id).await.unwrap();
        let after_stop = handle.get_snapshot().await.unwrap();
        assert_eq!(after_stop.tasks[0].status, TaskStatus::Completed);
        assert_eq!(after_stop.next_seq, completed.next_seq);
    }

    handle
        .update_task(id, task("after completion", true))
        .await
        .unwrap();
    let updated = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(updated.status, TaskStatus::Completed);
    assert_eq!(updated.configuration.revision, SpecRevision::new(2));
    assert_eq!(updated.result.as_ref().unwrap().context.revision, SpecRevision::new(1));

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn preparation_is_observable_before_scn_returns_and_available_to_other_subscribers() {
    check_preparation_is_observable_before_scn_returns_and_available_to_other_subscribers(
        Application::new,
    )
    .await;
}

#[tokio::test]
async fn ordered_preparation_is_observable_before_scn_returns_and_available_to_other_subscribers() {
    check_preparation_is_observable_before_scn_returns_and_available_to_other_subscribers(
        Application::new_ordered,
    )
    .await;
}

async fn check_preparation_is_observable_before_scn_returns_and_available_to_other_subscribers(
    prepare: Prepare,
) {
    use async_snmp::{Value, message::CommunityMessage};
    use tools_core::{
        monitor::{ScenarioBuildError, ScenarioFailure, TaskTransitionCause},
        snmp::{RawSnmpOidItem, SnmpError},
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
    let config = TaskConfig::try_new("prepare".to_string(), scenario, 3).unwrap();
    let application = prepare(
        AppConfig::default(),
        vec![InitialTask {
            config,
            limit: RunLimit::Runs(NonZeroU64::MIN),
        }],
    );
    let mut recorded = application.subscribe();
    let mut events = application.subscribe();
    let running = application.spawn();
    let handle = running.handle();
    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let fetching = matches!(&event.data, EventData::Preparation(progress)
            if progress.message == "Reading SCN: selected OIDs require a site identifier");
        observed.push(serde_json::to_value(event).unwrap());
        if fetching {
            break;
        }
    }
    let view = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(view.status, TaskStatus::Preparing);
    assert_eq!(view.run_limit, Some(RunLimit::Runs(NonZeroU64::MIN)));
    assert_eq!(view.completed_runs, 0);
    let progress = view.progress.as_ref().unwrap();
    assert_eq!(progress.message, "Reading SCN: selected OIDs require a site identifier");
    assert_eq!(
        serde_json::json!({
            "at": observed.last().unwrap()["data"]["at"],
            "message": observed.last().unwrap()["data"]["message"],
        }),
        serde_json::to_value(progress).unwrap()
    );
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
        let failed = matches!(event.data, EventData::TaskStateChanged(ref transition)
            if transition.current == TaskStatus::Restarting);
        assert!(
            !matches!(event.data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Active)
        );
        observed.push(serde_json::to_value(event).unwrap());
        if failed {
            break;
        }
    }
    let view = handle.get_snapshot().await.unwrap().tasks.remove(0);
    assert_eq!(view.status, TaskStatus::Restarting);
    assert!(view.progress.is_none());
    assert_eq!(view.metrics.executions, 0);
    assert!(matches!(
        &view.last_transition.as_ref().unwrap().cause,
        TaskTransitionCause::Failure(failure)
            if matches!(failure.as_ref(), ScenarioFailure::Preparation(
                ScenarioBuildError::Snmp(SnmpError::UnexpectedValueType { .. })
            ))
    ));

    running.shutdown().await.unwrap();
    while let Ok(event) = events.try_recv() {
        observed.push(serde_json::to_value(event).unwrap());
    }
    let mut history = Vec::new();
    while let Ok(event) = recorded.try_recv() {
        let json = serde_json::to_value(event).unwrap();
        let restored: MonitorEvent = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), json);
        history.push(json);
    }
    assert_eq!(history, observed);
}

#[tokio::test]
async fn ordered_subscription_recovers_from_lag_during_concurrent_changes() {
    use tokio::sync::broadcast::error::RecvError;

    let running = Application::new_ordered(AppConfig::default(), vec![]).spawn();
    let handle = running.handle();
    let first_config = task("first", false);
    let first = handle.add_task(first_config.clone()).await.unwrap();
    let mut slow = handle.subscribe_with_snapshot().await.unwrap();
    for _ in 0..1025 {
        handle
            .update_task(first, first_config.clone())
            .await
            .unwrap();
    }
    assert!(matches!(slow.events.recv().await, Err(RecvError::Lagged(_))));
    let (subscription, second) =
        tokio::join!(handle.subscribe_with_snapshot(), handle.add_task(task("second", true)),);
    let mut subscription = subscription.unwrap();
    let second = second.unwrap();
    assert_eq!(subscription.snapshot.tasks[0].configuration.revision, SpecRevision::new(1026));
    let mut seq = subscription.snapshot.next_seq;
    if !subscription
        .snapshot
        .tasks
        .iter()
        .any(|task| task.id == second)
    {
        let event = subscription.events.recv().await.unwrap();
        assert_eq!((event.metadata.seq, event.metadata.task_id), (seq, second));
        assert!(matches!(event.data, EventData::TaskRegistered(_)));
        seq += 1;
    }
    handle.remove_task(first).await.unwrap();
    let removed = subscription.events.recv().await.unwrap();
    assert_eq!((removed.metadata.seq, removed.metadata.task_id), (seq, first));
    assert!(matches!(removed.data, EventData::TaskRemoved));
    let snapshot = handle.get_snapshot().await.unwrap();
    assert_eq!(snapshot.next_seq, seq + 1);
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].id, second);
    running.shutdown().await.unwrap();
    assert!(matches!(subscription.events.recv().await, Err(RecvError::Closed)));
}
