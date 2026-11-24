use std::{num::NonZeroU64, time::Duration};

use futures_util::poll;
use tokio::sync::oneshot::error::TryRecvError;

use super::*;
use crate::{
    monitor::{
        OperationContext, OperationId, PollSchedule, ScenarioBuildError, ScenarioSpec, SnmpGetSpec,
    },
    polling::AttemptConfig,
};

fn config() -> TaskConfig {
    let request =
        SnmpGetSpec::new("127.0.0.1:161".parse().unwrap(), "public".parse().unwrap(), None, vec![]);
    let scenario = ScenarioSpec::snmp_get(
        request,
        PollSchedule::fixed_delay(Duration::from_secs(1)).unwrap(),
        AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap(),
    )
    .unwrap();
    TaskConfig::try_new("test".to_owned(), scenario, 1).unwrap()
}

struct ControlledRun {
    actor: OrderedOrchestrator,
    handle: OrchestratorHandle,
    context: ScenarioContext,
    finish: oneshot::Sender<ScenarioExit>,
    dropped: oneshot::Receiver<()>,
}

async fn controlled(limit: RunLimit) -> ControlledRun {
    let (mut actor, handle, _) =
        OrderedOrchestrator::new(ApplicationId::generate(), AppConfig::default(), vec![]);
    let id = actor.register(config());
    let entry = actor.entries.get_mut(&id).unwrap();
    entry.task.begin_run(limit);
    let generation = Generation::first();
    let context = ScenarioContext {
        task_id: id,
        generation,
        revision: entry.task.revision(),
    };
    let (finish, outcome) = oneshot::channel();
    let (lifetime, dropped) = oneshot::channel();
    let (mode, _rx) = watch::channel(RunMode::Running);
    entry.generation = Some(generation);
    entry.execution = Execution::Live(Box::new(LiveRun::spawn(
        &mut actor.workers,
        actor.tx.clone(),
        context,
        mode,
        move |stop| async move {
            let _lifetime = lifetime;
            tokio::select! {
                biased;
                _ = stop => ScenarioExit::Cancelled,
                result = outcome => result.expect("test sets the scenario outcome"),
            }
        },
    )));
    actor
        .transition(id, TaskStatus::Preparing, TaskTransitionCause::InitialStart, None)
        .await;
    ControlledRun {
        actor,
        handle,
        context,
        finish,
        dropped,
    }
}

fn execution(context: ScenarioContext) -> ScenarioExecution {
    let fixture = include_str!("../../../../tests/fixtures/monitor_operation.json");
    let event: MonitorEvent = serde_json::from_str(fixture).unwrap();
    let EventData::OperationFinished(operation) = event.data else {
        panic!("operation fixture")
    };
    ScenarioExecution::new(
        OperationContext::new(OperationId::new(0), context.generation, context.revision),
        *operation.report,
    )
}

async fn next(actor: &mut OrderedOrchestrator) {
    let message = tokio::time::timeout(Duration::from_secs(5), actor.rx.recv())
        .await
        .unwrap()
        .unwrap();
    actor.handle_message(message).await;
}

#[tokio::test(start_paused = true)]
async fn pause_before_failure_survives_backoff_until_explicit_resume() {
    let ControlledRun {
        mut actor,
        handle,
        context,
        finish,
        ..
    } = controlled(RunLimit::Unlimited).await;
    let id = context.task_id;
    let mut request = Box::pin(handle.pause_task(id));
    assert!(poll!(&mut request).is_pending());
    next(&mut actor).await;
    request.await.unwrap();
    assert!(
        finish
            .send(ScenarioExit::PreparationFailed(ScenarioBuildError::Timeout))
            .is_ok()
    );
    next(&mut actor).await;
    let snapshot = actor.snapshot().tasks.remove(0);
    assert_eq!(snapshot.status, TaskStatus::Paused);
    assert!(matches!(
        snapshot.last_failure,
        Some(ScenarioFailure::Preparation(ScenarioBuildError::Timeout))
    ));
    tokio::time::advance(Duration::from_secs(120)).await;
    assert!(actor.rx.try_recv().is_err());
    let mut resume = Box::pin(handle.resume_task(id));
    assert!(poll!(&mut resume).is_pending());
    next(&mut actor).await;
    resume.await.unwrap();
    assert!(actor.entries[&id].is_live(context.generation.next()));
}

#[tokio::test]
async fn final_result_already_in_mailbox_survives_stop() {
    let ControlledRun {
        mut actor,
        handle,
        context,
        finish,
        ..
    } = controlled(RunLimit::Runs(NonZeroU64::MIN)).await;
    let mut events = handle.subscribe();
    assert!(
        finish
            .send(ScenarioExit::Completed(Some(execution(context))))
            .is_ok()
    );
    actor.workers.join_next().await.unwrap().unwrap();
    let mut stop = Box::pin(handle.stop_task(context.task_id));
    assert!(poll!(&mut stop).is_pending());
    next(&mut actor).await;
    next(&mut actor).await;
    stop.await.unwrap();
    let view = actor.snapshot().tasks.remove(0);
    assert_eq!(view.completed_runs, 1);
    assert_eq!(view.status, TaskStatus::Completed);
    assert_eq!(view.result.unwrap().context.generation, context.generation);
    assert!(matches!(events.try_recv().unwrap().data, EventData::OperationFinished(_)));
    assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(change)
        if matches!(change.cause, TaskTransitionCause::LimitReached { limit: 1 })));
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn removal_waits_for_local_exit_while_another_task_accepts_commands() {
    let ControlledRun {
        mut actor,
        handle,
        context,
        mut dropped,
        finish: _finish,
    } = controlled(RunLimit::Unlimited).await;
    let mut remove = Box::pin(handle.remove_task(context.task_id));
    let mut add = Box::pin(handle.add_task(config()));
    assert!(poll!(&mut remove).is_pending());
    assert!(poll!(&mut add).is_pending());
    next(&mut actor).await;
    assert!(poll!(&mut remove).is_pending());
    next(&mut actor).await;
    assert_ne!(add.await.unwrap(), context.task_id);
    assert!(poll!(&mut remove).is_pending());
    next(&mut actor).await;
    remove.await.unwrap();
    assert_eq!(dropped.try_recv(), Err(TryRecvError::Closed));
    assert!(!actor.entries.contains_key(&context.task_id));
}

#[tokio::test]
async fn final_result_waiting_for_mailbox_capacity_survives_concurrent_stop() {
    let ControlledRun {
        mut actor,
        handle,
        context,
        finish,
        dropped,
    } = controlled(RunLimit::Runs(NonZeroU64::MIN)).await;
    let mut stop = Box::pin(handle.stop_task(context.task_id));
    assert!(poll!(&mut stop).is_pending());
    let remaining = actor.tx.capacity();
    let reserved = actor.tx.clone();
    let permits = reserved.reserve_many(remaining).await.unwrap();
    assert!(
        finish
            .send(ScenarioExit::Completed(Some(execution(context))))
            .is_ok()
    );
    assert!(dropped.await.is_err());
    next(&mut actor).await;
    assert!(poll!(&mut stop).is_pending());
    drop(permits);
    next(&mut actor).await;
    stop.await.unwrap();
    let view = actor.snapshot().tasks.remove(0);
    assert_eq!(view.completed_runs, 1);
    assert_eq!(view.status, TaskStatus::Completed);
}

#[tokio::test(start_paused = true)]
async fn queued_restart_from_before_pause_cannot_replace_a_resumed_run() {
    let ControlledRun {
        mut actor,
        handle,
        context,
        finish,
        ..
    } = controlled(RunLimit::Unlimited).await;
    let id = context.task_id;
    assert!(
        finish
            .send(ScenarioExit::PreparationFailed(ScenarioBuildError::Timeout))
            .is_ok()
    );
    next(&mut actor).await;
    assert_eq!(actor.entries[&id].status(), TaskStatus::Restarting);
    let timer = actor.rx.recv().await.unwrap();
    assert!(
        matches!(&timer, Message::Command(command) if matches!(**command, Command::Respawn { .. }))
    );
    let mut pause = Box::pin(handle.pause_task(id));
    let mut resume = Box::pin(handle.resume_task(id));
    assert!(poll!(&mut pause).is_pending());
    assert!(poll!(&mut resume).is_pending());
    next(&mut actor).await;
    next(&mut actor).await;
    pause.await.unwrap();
    resume.await.unwrap();
    assert!(actor.entries[&id].is_live(context.generation.next()));
    actor.handle_message(timer).await;
    assert!(actor.entries[&id].is_live(context.generation.next()));
}

#[tokio::test]
async fn shutdown_finishes_pending_stop_and_closes_deferred_start() {
    let ControlledRun {
        actor,
        handle,
        context,
        dropped,
        finish: _finish,
    } = controlled(RunLimit::Unlimited).await;
    let id = context.task_id;
    let mut events = handle.subscribe();
    let mut stop = Box::pin(handle.stop_task(id));
    let mut start = Box::pin(handle.start_task(id, RunLimit::Unlimited));
    let mut shutdown = Box::pin(handle.shutdown());
    assert!(poll!(&mut stop).is_pending());
    assert!(poll!(&mut start).is_pending());
    assert!(poll!(&mut shutdown).is_pending());
    let runtime = tokio::spawn(actor.run());
    shutdown.await.unwrap();
    stop.await.unwrap();
    assert!(matches!(start.await, Err(MonitorCommandError::ChannelClosed)));
    runtime.await.unwrap();
    assert!(dropped.await.is_err());
    assert!(
        matches!(events.recv().await.unwrap().data, EventData::TaskStateChanged(change) if change.current == TaskStatus::Stopped)
    );
    assert!(matches!(events.recv().await, Err(broadcast::error::RecvError::Closed)));
}

#[tokio::test]
async fn independent_scenario_completion_releases_an_unlimited_task() {
    let ControlledRun {
        mut actor,
        context,
        finish,
        ..
    } = controlled(RunLimit::Unlimited).await;
    assert!(finish.send(ScenarioExit::Completed(None)).is_ok());
    next(&mut actor).await;
    let view = actor.snapshot().tasks.remove(0);
    assert_eq!(view.status, TaskStatus::Completed);
    assert!(matches!(
        view.last_transition.unwrap().cause,
        TaskTransitionCause::ScenarioCompleted
    ));
    assert_eq!(view.completed_runs, 0);
    assert!(!actor.entries[&context.task_id].is_live(context.generation));
}

#[tokio::test]
async fn shutdown_rejects_queued_commands_while_history_is_draining() {
    let (sink, mut recorded) = mpsc::channel(1);
    let (actor, handle, _) = OrderedOrchestrator::new(
        ApplicationId::generate(),
        AppConfig {
            history_sink: Some(sink),
            ..AppConfig::default()
        },
        vec![],
    );
    let mut first = Box::pin(handle.add_task(config()));
    let mut second = Box::pin(handle.add_task(config()));
    let mut shutdown = Box::pin(handle.shutdown());
    let mut snapshot = Box::pin(handle.get_snapshot());
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());
    assert!(poll!(&mut shutdown).is_pending());
    assert!(poll!(&mut snapshot).is_pending());

    let runtime = tokio::spawn(actor.run());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), snapshot)
            .await
            .unwrap(),
        Err(MonitorCommandError::ChannelClosed)
    ));
    assert!(poll!(&mut shutdown).is_pending());
    for id in [first.await.unwrap(), second.await.unwrap()] {
        assert_eq!(recorded.recv().await.unwrap().metadata.task_id, id);
    }
    shutdown.await.unwrap();
    runtime.await.unwrap();
}
