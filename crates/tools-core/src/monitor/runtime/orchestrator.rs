use std::collections::HashMap;

use chrono::Utc;
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::{AbortHandle, JoinSet},
    time::{Instant, sleep_until},
};
use tracing::Instrument;

use super::{
    command::{Command, OrchestratorHandle},
    event_output::log_event,
};

use crate::monitor::{
    OperationFinished, OperationStarted, RestartPlan, ScenarioFailure, TaskCommandKind,
    TaskProgress, TaskTransition, TaskTransitionCause,
    event::{EventData, EventMeta, MonitorEvent},
    runtime::{
        ApplicationId, InitialTask, MonitorSubscription, error::MonitorCommandError,
        restart_policy::RestartPolicy,
    },
    scenario::{
        Generation, PanicStage, RunBudget, RunMode, ScenarioContext, ScenarioReport, ScenarioSpec,
        run_scenario,
    },
    snapshot::MonitorSnapshot,
    task::{
        RunLimit, SpecRevision, Task, TaskConfig, TaskDescription, TaskId, TaskSnapshot, TaskStatus,
    },
};

struct TaskEntry {
    task: Task,
    state: TaskStatus,
    progress: Option<TaskProgress>,
    last_transition: Option<TaskTransition>,
    last_failure: Option<ScenarioFailure>,
    attempts: u32,
    next_generation: Generation,
    generation: Generation,
    scenario: Option<ScenarioHandle>,
    finishing_scenario: Option<AbortHandle>,
}

impl TaskEntry {
    fn transition(
        &mut self,
        current: TaskStatus,
        cause: TaskTransitionCause,
        restart: Option<RestartPlan>,
    ) -> Option<TaskTransition> {
        if self.state == current {
            return None;
        }
        let run_limit = match cause {
            TaskTransitionCause::InitialStart
            | TaskTransitionCause::Command(TaskCommandKind::Start) => self.task.run_limit(),
            _ => None,
        };
        let transition = TaskTransition {
            at: Utc::now(),
            previous: self.state,
            current,
            cause,
            run_limit,
            restart,
        };
        self.state = current;
        if current != TaskStatus::Preparing {
            self.progress = None;
        }
        self.last_transition = Some(transition.clone());
        Some(transition)
    }
}

struct ScenarioHandle {
    generation: Generation,
    abort: AbortHandle,
    mode_tx: watch::Sender<RunMode>,
}

impl ScenarioHandle {
    fn request_mode(&self, mode: RunMode) {
        self.mode_tx.send_if_modified(|current| {
            if *current == mode {
                return false;
            }
            *current = mode;
            true
        });
    }
}

pub(super) struct Orchestrator {
    next_id: u64,
    initial_tasks: Vec<(TaskId, RunLimit)>,
    entries: HashMap<TaskId, TaskEntry>,
    cmd_rx: mpsc::Receiver<Command>,
    cmd_tx: mpsc::Sender<Command>,
    report_rx: mpsc::Receiver<ScenarioReport>,
    report_tx: mpsc::Sender<ScenarioReport>,
    live_tx: broadcast::Sender<MonitorEvent>,
    scenarios: JoinSet<()>,
    restarts: JoinSet<()>,
    policy: RestartPolicy,
    seq: u64,
    application_id: ApplicationId,
}

impl Orchestrator {
    pub fn new(
        restart: RestartPolicy,
        application_id: ApplicationId,
    ) -> (Self, OrchestratorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (report_tx, report_rx) = mpsc::channel(256);
        let (live_tx, _) = broadcast::channel(1024);

        let this = Self {
            next_id: 1,
            initial_tasks: Vec::new(),
            entries: HashMap::new(),
            cmd_rx,
            cmd_tx: cmd_tx.clone(),
            report_rx,
            report_tx,
            live_tx: live_tx.clone(),
            scenarios: JoinSet::new(),
            restarts: JoinSet::new(),
            policy: restart,
            seq: 0,
            application_id,
        };
        (this, OrchestratorHandle::current(cmd_tx, live_tx.downgrade()))
    }

    pub(super) fn prepare_tasks(&mut self, tasks: Vec<InitialTask>) -> Vec<TaskId> {
        let mut ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            let id = self.register_task(task.config);
            self.initial_tasks.push((id, task.limit));
            ids.push(id);
        }
        ids
    }

    fn register_task(&mut self, spec: TaskConfig) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        self.entries.insert(
            id,
            TaskEntry {
                task: Task::new(id, spec),
                state: TaskStatus::Idle,
                progress: None,
                last_transition: None,
                last_failure: None,
                attempts: 0,
                next_generation: Generation::first(),
                generation: Generation::first(),
                scenario: None,
                finishing_scenario: None,
            },
        );
        id
    }

    #[tracing::instrument(name = "monitor", skip_all, fields(application_id = %self.application_id))]
    pub async fn run(mut self) {
        tracing::info!(task_count = self.entries.len(), "monitor started");
        self.start_initial_tasks().await;
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(c) => {
                        if !self.on_command(c).await {
                            break;
                        }
                    }
                    None => break,
                },
                Some(rep) = self.report_rx.recv() => self.on_report(rep).await,
                Some(result) = self.scenarios.join_next() => {
                    log_background_result(result, "scenario", self.application_id);
                },
                Some(result) = self.restarts.join_next() => {
                    log_background_result(result, "restart_timer", self.application_id);
                },
            }
        }
        tracing::info!("monitor stopping");
        self.restarts.shutdown().await;
        self.scenarios.shutdown().await;
        tracing::info!("monitor stopped");
    }

    async fn start_initial_tasks(&mut self) {
        let initial_tasks = std::mem::take(&mut self.initial_tasks);
        for &(id, _) in &initial_tasks {
            self.emit_task_registered(id).await;
        }
        for (id, limit) in initial_tasks {
            self.start_with_cause(id, limit, TaskTransitionCause::InitialStart)
                .await
                .expect("initial task is registered");
        }
    }

    async fn emit_task_registered(&mut self, id: TaskId) {
        let entry = self
            .entries
            .get(&id)
            .expect("task was registered before publication");
        let description = Box::new(TaskDescription::from(entry.task.config()));
        self.emit(id, EventData::TaskRegistered(description));
    }

    async fn on_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::AddTask { spec, reply } => {
                let id = self.register_task(spec);
                self.emit_task_registered(id).await;
                let _ = reply.send(Ok(id));
                true
            }
            Command::RemoveTask { task_id, reply } => {
                let res = self.remove(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::StartTask {
                task_id,
                limit,
                reply,
            } => {
                let res = self.start(task_id, limit).await;
                let _ = reply.send(res);
                true
            }
            Command::PauseTask { task_id, reply } => {
                let res = self.pause(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::ResumeTask { task_id, reply } => {
                let res = self.resume(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::StopTask { task_id, reply } => {
                let res = self.stop(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::UpdateTask {
                task_id,
                spec,
                reply,
            } => {
                let res = self.update(task_id, spec).await;
                let _ = reply.send(res);
                true
            }
            Command::GetSnapshot { reply } => {
                let _ = reply.send(self.snapshot());
                true
            }
            Command::SubscribeWithSnapshot { reply } => {
                let events = self.live_tx.subscribe();
                let snapshot = self.snapshot();
                let _ = reply.send(MonitorSubscription { snapshot, events });
                true
            }
            Command::Shutdown { reply } => {
                let mut running: Vec<_> = self
                    .entries
                    .iter()
                    .filter_map(|(&id, entry)| {
                        matches!(
                            entry.state,
                            TaskStatus::Preparing
                                | TaskStatus::Active
                                | TaskStatus::Paused
                                | TaskStatus::Restarting
                        )
                        .then_some(id)
                    })
                    .collect();
                running.sort_unstable();
                for id in running {
                    self.stop_with_cause(id, TaskTransitionCause::Shutdown)
                        .await
                        .expect("task exists until shutdown clears the registry");
                }
                self.entries.clear();
                let _ = reply.send(());
                false
            }
            Command::Respawn {
                task_id,
                generation,
            } => {
                self.maybe_respawn(task_id, generation).await;
                true
            }
        }
    }

    async fn start(&mut self, id: TaskId, limit: RunLimit) -> Result<(), MonitorCommandError> {
        self.start_with_cause(id, limit, TaskTransitionCause::Command(TaskCommandKind::Start))
            .await
    }

    async fn start_with_cause(
        &mut self,
        id: TaskId,
        limit: RunLimit,
        cause: TaskTransitionCause,
    ) -> Result<(), MonitorCommandError> {
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))?;
        if !matches!(entry.state, TaskStatus::Idle | TaskStatus::Stopped | TaskStatus::Completed) {
            return if entry.task.run_limit() == Some(limit) {
                Ok(())
            } else {
                Err(MonitorCommandError::TaskAlreadyRunning(id))
            };
        }
        entry.task.begin_run(limit);
        entry.last_failure = None;
        entry.attempts = 0;
        self.spawn_scenario(id, cause).await;
        Ok(())
    }

    async fn pause(&mut self, id: TaskId) -> Result<(), MonitorCommandError> {
        let entry = self
            .entries
            .get(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))?;
        if let Some(scenario) = &entry.scenario {
            scenario.request_mode(RunMode::Paused);
        } else if matches!(
            entry.state,
            TaskStatus::Preparing | TaskStatus::Active | TaskStatus::Restarting
        ) {
            self.transition(
                id,
                TaskStatus::Paused,
                TaskTransitionCause::Command(TaskCommandKind::Pause),
                None,
            )
            .await;
        }
        Ok(())
    }

    async fn resume(&mut self, id: TaskId) -> Result<(), MonitorCommandError> {
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))?;
        if let Some(scenario) = &entry.scenario {
            scenario.request_mode(RunMode::Running);
        } else if entry.state == TaskStatus::Paused {
            entry.attempts = 0;
            self.spawn_scenario(id, TaskTransitionCause::Command(TaskCommandKind::Resume))
                .await;
        }
        Ok(())
    }

    async fn stop(&mut self, id: TaskId) -> Result<(), MonitorCommandError> {
        self.stop_with_cause(id, TaskTransitionCause::Command(TaskCommandKind::Stop))
            .await
    }

    async fn stop_with_cause(
        &mut self,
        id: TaskId,
        cause: TaskTransitionCause,
    ) -> Result<(), MonitorCommandError> {
        let (stopped, scenario) = {
            let entry = self
                .entries
                .get_mut(&id)
                .ok_or(MonitorCommandError::TaskNotFound(id))?;
            let stopped = !matches!(
                entry.state,
                TaskStatus::Idle | TaskStatus::Stopped | TaskStatus::Completed
            );
            if stopped {
                entry.attempts = 0;
            }
            let scenario = entry.scenario.take().map(|scenario| scenario.abort);
            (stopped, scenario.or_else(|| entry.finishing_scenario.take()))
        };
        if let Some(scenario) = scenario {
            if stopped {
                scenario.abort();
            }
            self.wait_for_scenario(&scenario).await;
        }
        if stopped {
            self.transition(id, TaskStatus::Stopped, cause, None).await;
        }
        Ok(())
    }

    async fn update(&mut self, id: TaskId, spec: TaskConfig) -> Result<(), MonitorCommandError> {
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))?;
        if !matches!(entry.state, TaskStatus::Idle | TaskStatus::Stopped | TaskStatus::Completed) {
            return Err(MonitorCommandError::TaskUpdateWhileRunning {
                task_id: id,
                status: entry.state,
            });
        }
        entry.task.update_config(spec);
        let description = Box::new(TaskDescription::from(entry.task.config()));
        self.emit(id, EventData::TaskConfigurationChanged(description));
        Ok(())
    }

    async fn wait_for_scenario(&mut self, scenario: &AbortHandle) {
        while !scenario.is_finished() {
            let result = self
                .scenarios
                .join_next()
                .await
                .expect("running scenario is tracked");
            log_background_result(result, "scenario", self.application_id);
        }
    }

    async fn remove(&mut self, id: TaskId) -> Result<(), MonitorCommandError> {
        let entry = self
            .entries
            .remove(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))?;
        if let Some(scenario) = &entry.scenario {
            scenario.abort.abort();
        }
        let generation = entry.generation;
        let revision = entry.task.revision();
        let metadata = self.next_meta(id, generation, revision);
        self.emit_event(MonitorEvent {
            metadata,
            data: EventData::TaskRemoved,
        });
        Ok(())
    }

    async fn on_report(&mut self, rep: ScenarioReport) {
        let (task_id, generation) = rep.key();
        let Some(entry) = self.entries.get(&task_id) else {
            tracing::trace!(%task_id, ?generation, "report ignored for missing task");
            return;
        };
        if entry.scenario.as_ref().map(|scenario| scenario.generation) != Some(generation) {
            tracing::trace!(
                %task_id,
                ?generation,
                active_generation = ?entry.scenario.as_ref().map(|scenario| scenario.generation),
                "report ignored for inactive generation"
            );
            return;
        }

        match rep {
            ScenarioReport::Completed { .. } => {
                let entry = self.entries.get_mut(&task_id).expect("validated task");
                entry.finishing_scenario = entry.scenario.take().map(|scenario| scenario.abort);
                self.transition(
                    task_id,
                    TaskStatus::Completed,
                    TaskTransitionCause::ScenarioCompleted,
                    None,
                )
                .await;
            }
            ScenarioReport::Progress { progress, .. } => {
                let entry = self.entries.get_mut(&task_id).expect("validated task");
                if entry.state != TaskStatus::Preparing {
                    return;
                }
                entry.progress = Some(progress.clone());
                self.emit(task_id, EventData::Preparation(progress));
            }
            ScenarioReport::OperationStarted {
                context,
                purpose,
                started_at,
                config,
                ..
            } => {
                self.emit(
                    task_id,
                    EventData::OperationStarted(Box::new(OperationStarted {
                        context,
                        purpose,
                        started_at,
                        config,
                    })),
                );
            }
            ScenarioReport::Built { .. } => {
                let entry = self.entries.get_mut(&task_id).expect("validated task");
                entry.attempts = 0;
                entry.last_failure = None;
                self.transition(
                    task_id,
                    TaskStatus::Active,
                    TaskTransitionCause::ScenarioReady,
                    None,
                )
                .await;
            }
            ScenarioReport::BuildFailed { error, .. } => {
                self.recover(task_id, ScenarioFailure::Preparation(error), None)
                    .await;
            }
            ScenarioReport::BuildPanicked { reason, .. } => {
                self.recover(
                    task_id,
                    ScenarioFailure::Panicked {
                        stage: PanicStage::Build,
                        reason,
                    },
                    None,
                )
                .await;
            }
            ScenarioReport::RunFinished {
                execution,
                limit_reached,
                ..
            } => {
                let entry = self.entries.get_mut(&task_id).expect("validated task");
                entry.task.apply_execution(execution.clone());
                entry.task.record_completed_run();
                entry.last_failure = None;
                self.emit(
                    task_id,
                    EventData::OperationFinished(OperationFinished {
                        context: execution.context,
                        report: Box::new(execution.report),
                    }),
                );
                if limit_reached {
                    self.complete_at_limit(task_id).await;
                }
            }
            ScenarioReport::ExecutionFailed {
                error, execution, ..
            } => {
                self.recover(task_id, ScenarioFailure::Execution(error), Some(execution))
                    .await;
            }
            ScenarioReport::ExecutionPanicked { reason, .. } => {
                self.recover(
                    task_id,
                    ScenarioFailure::Panicked {
                        stage: PanicStage::Execution,
                        reason,
                    },
                    None,
                )
                .await;
            }
            ScenarioReport::Panicked { reason, .. } => {
                self.recover(
                    task_id,
                    ScenarioFailure::Panicked {
                        stage: PanicStage::Runtime,
                        reason,
                    },
                    None,
                )
                .await;
            }
            ScenarioReport::Paused { .. } => {
                self.transition(
                    task_id,
                    TaskStatus::Paused,
                    TaskTransitionCause::Command(TaskCommandKind::Pause),
                    None,
                )
                .await;
            }
            ScenarioReport::Resumed { .. } => {
                self.transition(
                    task_id,
                    TaskStatus::Active,
                    TaskTransitionCause::Command(TaskCommandKind::Resume),
                    None,
                )
                .await;
            }
        }
    }

    async fn recover(
        &mut self,
        task_id: TaskId,
        failure: ScenarioFailure,
        execution: Option<crate::monitor::ScenarioExecution>,
    ) {
        let entry = self.entries.get_mut(&task_id).expect("validated task");
        entry.last_failure = Some(failure.clone());
        entry.finishing_scenario = entry.scenario.take().map(|scenario| scenario.abort);
        entry.attempts += 1;
        let delay = self.policy.delay(entry.attempts);
        let deadline = Instant::now() + delay;
        let plan = RestartPlan {
            attempt: entry.attempts,
            scheduled_at: Utc::now() + delay,
        };
        let generation = entry.generation;
        let data = execution.map(|execution| {
            entry.task.store_execution(execution.clone());
            EventData::OperationFinished(OperationFinished {
                context: execution.context,
                report: Box::new(execution.report),
            })
        });
        if let Some(data) = data {
            self.emit(task_id, data);
        }
        self.transition(
            task_id,
            TaskStatus::Restarting,
            TaskTransitionCause::Failure(Box::new(failure)),
            Some(plan),
        )
        .await;
        self.schedule_respawn(task_id, generation, deadline);
    }

    async fn transition(
        &mut self,
        task_id: TaskId,
        current: TaskStatus,
        cause: TaskTransitionCause,
        restart: Option<RestartPlan>,
    ) {
        let transition = self
            .entries
            .get_mut(&task_id)
            .and_then(|entry| entry.transition(current, cause, restart));
        if let Some(transition) = transition {
            self.emit(task_id, EventData::TaskStateChanged(Box::new(transition)));
        }
    }

    async fn complete_at_limit(&mut self, task_id: TaskId) {
        let entry = self.entries.get_mut(&task_id).expect("task is registered");
        let Some(RunLimit::Runs(limit)) = entry.task.run_limit() else {
            unreachable!("only a finite limit can be reached");
        };
        entry.finishing_scenario = entry.scenario.take().map(|scenario| scenario.abort);
        self.transition(
            task_id,
            TaskStatus::Completed,
            TaskTransitionCause::LimitReached { limit: limit.get() },
            None,
        )
        .await;
    }

    async fn spawn_scenario(&mut self, task_id: TaskId, cause: TaskTransitionCause) {
        let finishing = self
            .entries
            .get_mut(&task_id)
            .expect("task is registered")
            .finishing_scenario
            .take();
        if let Some(finishing) = finishing {
            self.wait_for_scenario(&finishing).await;
        }
        let entry = self.entries.get_mut(&task_id).expect("task is registered");
        let limit = entry.task.run_limit().expect("task execution has started");
        let budget = RunBudget::new(limit, entry.task.completed_runs());
        if budget.is_exhausted() {
            self.complete_at_limit(task_id).await;
            return;
        }
        let generation = entry.next_generation;
        entry.next_generation = generation.next();
        entry.generation = generation;
        entry.progress = None;
        let scenario = entry.task.config().scenario().clone();
        let revision = entry.task.revision();
        let scenario_kind = match &scenario {
            ScenarioSpec::SnmpGet(_) => "snmp_get",
            ScenarioSpec::SnmpSet(_) => "snmp_set",
        };
        let span = tracing::info_span!(
            "scenario",
            task_id = task_id.0,
            ?generation,
            revision = entry.task.revision().0,
            task_name = entry.task.config().name(),
            scenario_kind,
            device = %scenario.target(),
        );
        let (mode_tx, mode_rx) = watch::channel(RunMode::Running);
        let report_tx = self.report_tx.clone();
        let abort = self.scenarios.spawn(
            run_scenario(
                scenario,
                ScenarioContext {
                    task_id,
                    generation,
                    revision,
                },
                budget,
                report_tx,
                mode_rx,
            )
            .instrument(span),
        );
        entry.scenario = Some(ScenarioHandle {
            generation,
            abort,
            mode_tx,
        });
        self.transition(task_id, TaskStatus::Preparing, cause, None)
            .await;
    }

    fn schedule_respawn(&mut self, task_id: TaskId, generation: Generation, deadline: Instant) {
        let cmd_tx = self.cmd_tx.clone();
        let span = tracing::info_span!("restart_timer", task_id = task_id.0, ?generation);
        self.restarts.spawn(
            async move {
                sleep_until(deadline).await;
                let _ = cmd_tx
                    .send(Command::Respawn {
                        task_id,
                        generation,
                    })
                    .await;
            }
            .instrument(span),
        );
    }

    async fn maybe_respawn(&mut self, task_id: TaskId, generation: Generation) {
        let should = matches!(
            self.entries.get(&task_id),
            Some(e) if e.state == TaskStatus::Restarting
                && e.scenario.is_none()
                && e.generation == generation
        );
        if should {
            let attempt = self.entries.get(&task_id).map_or(0, |e| e.attempts);
            self.spawn_scenario(task_id, TaskTransitionCause::RestartDue { attempt })
                .await;
        } else {
            tracing::trace!(%task_id, ?generation, "restart ignored for inactive generation or state");
        }
    }

    fn emit(&mut self, task_id: TaskId, data: EventData) {
        let Some((generation, revision)) = self
            .entries
            .get(&task_id)
            .map(|e| (e.generation, e.task.revision()))
        else {
            return;
        };
        let metadata = self.next_meta(task_id, generation, revision);
        self.emit_event(MonitorEvent { metadata, data });
    }

    fn next_meta(
        &mut self,
        task_id: TaskId,
        generation: Generation,
        revision: SpecRevision,
    ) -> EventMeta {
        let seq = self.seq;
        self.seq += 1;
        EventMeta {
            seq,
            at: Utc::now(),
            application_id: self.application_id,
            task_id,
            generation,
            revision,
        }
    }

    fn emit_event(&self, ev: MonitorEvent) {
        log_event(&ev);
        let _ = self.live_tx.send(ev);
    }

    fn task_snapshot(&self, entry: &TaskEntry) -> TaskSnapshot {
        entry.task.snapshot(
            entry.state,
            entry.progress.as_ref(),
            entry.last_transition.as_ref(),
            entry.last_failure.as_ref(),
        )
    }

    pub(super) fn snapshot(&self) -> MonitorSnapshot {
        let mut tasks: Vec<TaskSnapshot> = self
            .entries
            .values()
            .map(|entry| self.task_snapshot(entry))
            .collect();
        tasks.sort_by_key(|v| v.id);
        MonitorSnapshot {
            application_id: self.application_id,
            next_seq: self.seq,
            tasks,
        }
    }
}

fn log_background_result(
    result: Result<(), tokio::task::JoinError>,
    operation: &str,
    application_id: ApplicationId,
) {
    if let Err(error) = result {
        if error.is_cancelled() {
            tracing::debug!(%application_id, operation, tokio_task_id = %error.id(), "background task cancelled");
        } else {
            tracing::error!(%application_id, operation, tokio_task_id = %error.id(), %error, "background task failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::monitor::OperationReport;
    use chrono::DateTime;
    use tokio::sync::oneshot;
    use tokio::time::Duration;

    use super::*;
    use crate::{
        monitor::{
            scenario::{
                PollSchedule, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError,
                ScenarioOutcome, ScenarioSpec, SnmpGetScenarioSpec, SnmpSetScenarioSpec,
            },
            task::ExecutionMetrics,
        },
        polling::{AttemptConfig, AttemptOutcome, AttemptReport, PollOutcome, PollReport},
        snmp::{SnmpError, SnmpGetSpec, SnmpSetSpec},
    };

    fn active_task(limit: u64) -> (Orchestrator, broadcast::Receiver<MonitorEvent>, TaskId) {
        let limit = std::num::NonZeroU64::new(limit).map_or(RunLimit::Unlimited, RunLimit::Runs);
        let (mut orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let events = handle.subscribe();
        let task_id = TaskId(1);
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(10), 2, Duration::from_millis(1)).unwrap();
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
        let mut task =
            Task::new(task_id, TaskConfig::try_new("test".to_string(), scenario, 3).unwrap());
        task.begin_run(limit);
        let (mode_tx, _mode_rx) = watch::channel(RunMode::Running);
        let abort = orchestrator.scenarios.spawn(std::future::pending());
        orchestrator.entries.insert(
            task_id,
            TaskEntry {
                task,
                state: TaskStatus::Active,
                progress: None,
                last_transition: None,
                last_failure: None,
                attempts: 0,
                next_generation: Generation::first().next(),
                generation: Generation::first(),
                scenario: Some(ScenarioHandle {
                    generation: Generation::first(),
                    abort,
                    mode_tx,
                }),
                finishing_scenario: None,
            },
        );
        (orchestrator, events, task_id)
    }

    fn execution(at: i64, success: bool) -> ScenarioExecution {
        let finished_at = DateTime::from_timestamp(at, 0).unwrap();
        let elapsed = Duration::from_millis(if success { 50 } else { 80 });
        ScenarioExecution::new(
            crate::monitor::OperationContext::new(
                crate::monitor::OperationId::new(0),
                Generation::first(),
                SpecRevision::new(1),
            ),
            OperationReport::from_poll(
                PollReport {
                    started_at: finished_at - elapsed,
                    finished_at,
                    elapsed,
                    config: AttemptConfig::try_new(Duration::from_millis(100), 0, Duration::ZERO)
                        .unwrap(),
                    attempts: vec![AttemptReport {
                        started_after: Duration::ZERO,
                        elapsed,
                        outcome: if success {
                            AttemptOutcome::Succeeded
                        } else {
                            AttemptOutcome::TimedOut
                        },
                    }],
                    outcome: if success {
                        PollOutcome::Succeeded(vec![])
                    } else {
                        PollOutcome::RetryLimitReached
                    },
                },
                ScenarioOutcome::SnmpGet,
            ),
        )
    }

    fn set_config(name: &str) -> TaskConfig {
        let request = SnmpSetSpec::from_raw(
            "127.0.0.2".to_string(),
            1161,
            None,
            "private".to_string(),
            None,
            vec![],
        )
        .unwrap();
        let attempt = AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap();
        let scenario = ScenarioSpec::SnmpSet(
            SnmpSetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(2),
                },
                attempt,
            )
            .unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, 3).unwrap()
    }

    fn control_scenario(orchestrator: &mut Orchestrator, task_id: TaskId) {
        let entry = orchestrator.entries.get_mut(&task_id).unwrap();
        if let Some(scenario) = entry.scenario.take() {
            scenario.abort.abort();
        }
        let (mode_tx, _mode_rx) = watch::channel(RunMode::Running);
        let abort = orchestrator.scenarios.spawn(std::future::pending());
        entry.scenario = Some(ScenarioHandle {
            generation: entry.generation,
            abort,
            mode_tx,
        });
    }

    async fn fail_scenario(orchestrator: &mut Orchestrator, task_id: TaskId) {
        let entry = &orchestrator.entries[&task_id];
        let generation = entry.generation;
        entry.scenario.as_ref().unwrap().abort.abort();
        orchestrator
            .on_report(ScenarioReport::BuildFailed {
                task_id,
                generation,
                error: ScenarioBuildError::Other("unavailable".to_string()),
            })
            .await;
    }

    async fn wait_for_abort(abort: tokio::task::AbortHandle) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn execute(
        orchestrator: &mut Orchestrator,
        task_id: TaskId,
        execution: ScenarioExecution,
    ) {
        let task = &orchestrator.entries[&task_id].task;
        let limit_reached = matches!(task.run_limit(), Some(RunLimit::Runs(limit))
            if task.completed_runs() + 1 == limit.get());
        orchestrator
            .on_report(ScenarioReport::RunFinished {
                task_id,
                generation: Generation::first(),
                execution,
                limit_reached,
            })
            .await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_scenarios_and_restart_tasks() {
        let (mut orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let scenario = orchestrator.scenarios.spawn(std::future::pending());
        let restart = orchestrator.restarts.spawn(std::future::pending());
        let runtime = tokio::spawn(orchestrator.run());

        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
        assert!(scenario.is_finished());
        assert!(restart.is_finished());
    }

    #[tokio::test]
    async fn aborting_runtime_cancels_scenarios_and_restart_tasks() {
        let (mut orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let scenario = orchestrator.scenarios.spawn(std::future::pending());
        let restart = orchestrator.restarts.spawn(std::future::pending());
        let runtime = tokio::spawn(orchestrator.run());
        handle.get_snapshot().await.unwrap();

        runtime.abort();
        assert!(runtime.await.unwrap_err().is_cancelled());
        wait_for_abort(scenario).await;
        wait_for_abort(restart).await;
    }

    #[tokio::test]
    async fn subscription_snapshot_contains_existing_tasks_and_excludes_past_events() {
        let application_id = ApplicationId::generate();
        let (orchestrator, handle) = Orchestrator::new(RestartPolicy::default(), application_id);
        let runtime = tokio::spawn(orchestrator.run());
        let first = handle.add_task(set_config("first")).await.unwrap();
        let second = handle.add_task(set_config("second")).await.unwrap();

        let MonitorSubscription {
            snapshot,
            mut events,
        } = handle.subscribe_with_snapshot().await.unwrap();
        assert_eq!(snapshot.application_id, application_id);
        assert_eq!(snapshot.next_seq, 2);
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, first);
        assert_eq!(snapshot.tasks[1].id, second);
        assert!(matches!(events.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        let third = handle.add_task(set_config("third")).await.unwrap();
        let event = events.recv().await.unwrap();
        assert_eq!(event.metadata.application_id, snapshot.application_id);
        assert_eq!(event.metadata.seq, snapshot.next_seq);
        assert_eq!(event.metadata.task_id, third);
        assert!(matches!(event.data, EventData::TaskRegistered(_)));
        assert_eq!(snapshot.tasks.len(), 2);

        let next = handle.subscribe_with_snapshot().await.unwrap();
        assert_eq!(next.snapshot.next_seq, snapshot.next_seq + 1);
        assert_eq!(next.snapshot.tasks.len(), 3);
        handle.remove_task(first).await.unwrap();
        let event = events.recv().await.unwrap();
        assert_eq!(event.metadata.seq, next.snapshot.next_seq);
        assert_eq!(event.metadata.task_id, first);
        assert!(matches!(event.data, EventData::TaskRemoved));
        let current = handle.get_snapshot().await.unwrap();
        assert_eq!(current.application_id, snapshot.application_id);
        assert_eq!(current.next_seq, event.metadata.seq + 1);
        assert_eq!(current.tasks.len(), 2);
        assert_eq!(current.tasks[0].id, second);
        assert_eq!(current.tasks[1].id, third);

        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_captures_changes_queued_while_it_is_opening() {
        let (orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let runtime = tokio::spawn(orchestrator.run());
        let (subscription, task_id) = tokio::join!(
            handle.subscribe_with_snapshot(),
            handle.add_task(set_config("concurrent")),
        );
        let mut subscription = subscription.unwrap();
        let task_id = task_id.unwrap();

        if subscription
            .snapshot
            .tasks
            .iter()
            .any(|task| task.id == task_id)
        {
            assert_eq!(subscription.snapshot.next_seq, 1);
            assert!(matches!(
                subscription.events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));
        } else {
            let event = subscription.events.try_recv().unwrap();
            assert_eq!(event.metadata.seq, subscription.snapshot.next_seq);
            assert_eq!(event.metadata.task_id, task_id);
            assert!(matches!(event.data, EventData::TaskRegistered(_)));
        }
        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_closes_when_the_orchestrator_stops_with_handles_alive() {
        let (orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let runtime = tokio::spawn(orchestrator.run());
        let clone = handle.clone();
        let mut subscription = handle.subscribe_with_snapshot().await.unwrap();
        let mut raw_events = clone.subscribe();
        assert!(subscription.snapshot.tasks.is_empty());
        assert_eq!(subscription.snapshot.next_seq, 0);

        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
        assert!(matches!(
            subscription.events.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(matches!(raw_events.recv().await, Err(broadcast::error::RecvError::Closed)));
        assert!(matches!(
            clone.subscribe().recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(matches!(
            clone.subscribe_with_snapshot().await,
            Err(MonitorCommandError::ChannelClosed)
        ));
    }

    #[tokio::test]
    async fn subscription_snapshot_recovers_state_after_lagging() {
        let (orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), ApplicationId::generate());
        let runtime = tokio::spawn(orchestrator.run());
        let task_id = handle.add_task(set_config("initial")).await.unwrap();
        let mut subscription = handle.subscribe_with_snapshot().await.unwrap();
        for _ in 0..1025 {
            handle
                .update_task(task_id, set_config("updated"))
                .await
                .unwrap();
        }
        assert!(matches!(
            subscription.events.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        let snapshot = handle.get_snapshot().await.unwrap();
        assert_eq!(snapshot.application_id, subscription.snapshot.application_id);
        assert_eq!(snapshot.next_seq, 1026);
        assert_eq!(snapshot.tasks[0].configuration.description.name, "updated");
        assert_eq!(snapshot.tasks[0].configuration.revision, SpecRevision::new(1026));
        while let Ok(event) = subscription.events.try_recv() {
            assert!(event.metadata.seq < snapshot.next_seq);
        }

        handle.remove_task(task_id).await.unwrap();
        let event = subscription.events.recv().await.unwrap();
        assert_eq!(event.metadata.seq, snapshot.next_seq);
        assert!(matches!(event.data, EventData::TaskRemoved));
        assert!(handle.get_snapshot().await.unwrap().tasks.is_empty());
        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
    }

    #[tokio::test]
    async fn events_share_application_id_and_sequence_across_tasks() {
        let application_id = ApplicationId::generate();
        let (mut orchestrator, handle) =
            Orchestrator::new(RestartPolicy::default(), application_id);
        let mut events = handle.subscribe();
        let mut task_ids = Vec::new();
        for name in ["first", "second"] {
            let (reply, response) = oneshot::channel();
            assert!(
                orchestrator
                    .on_command(Command::AddTask {
                        spec: set_config(name),
                        reply,
                    })
                    .await
            );
            task_ids.push(response.await.unwrap().unwrap());
        }
        orchestrator
            .update(task_ids[0], set_config("updated"))
            .await
            .unwrap();
        orchestrator.remove(task_ids[1]).await.unwrap();

        for (seq, (task_id, revision)) in [
            (task_ids[0], SpecRevision::new(1)),
            (task_ids[1], SpecRevision::new(1)),
            (task_ids[0], SpecRevision::new(2)),
            (task_ids[1], SpecRevision::new(1)),
        ]
        .into_iter()
        .enumerate()
        {
            let event = events.try_recv().unwrap();
            assert_eq!(event.metadata.application_id, application_id);
            assert_eq!(event.metadata.seq, u64::try_from(seq).unwrap());
            assert_eq!(event.metadata.task_id, task_id);
            assert_eq!(event.metadata.revision, revision);
            assert_eq!(event.metadata.generation, Generation::first());
            match seq {
                0 | 1 => assert!(matches!(event.data, EventData::TaskRegistered(_))),
                2 => assert!(matches!(event.data, EventData::TaskConfigurationChanged(_))),
                3 => assert!(matches!(event.data, EventData::TaskRemoved)),
                _ => unreachable!(),
            }
        }
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn operation_start_publishes_context_without_creating_a_result_or_counting_attempts() {
        let (mut orchestrator, mut events, task_id) = active_task(1);
        let execution = execution(100, true);
        let ScenarioOutcome::SnmpGet(report) = execution.report.detail().unwrap() else {
            unreachable!()
        };
        orchestrator
            .on_report(ScenarioReport::OperationStarted {
                task_id,
                context: execution.context,
                purpose: "Read controller state".to_string(),
                started_at: execution.report.started_at,
                config: report.config,
            })
            .await;
        let started = events.try_recv().unwrap();
        let EventData::OperationStarted(operation) = started.data else {
            panic!("expected operation start");
        };
        let OperationStarted {
            context,
            started_at,
            config,
            ..
        } = *operation;
        assert_eq!(context, execution.context);
        assert_eq!(started_at, execution.report.started_at);
        assert_eq!(config, report.config);
        assert_eq!(started.metadata.generation, context.generation);
        assert_eq!(started.metadata.revision, context.revision);
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Active);
        assert_eq!(snapshot.tasks[0].metrics, ExecutionMetrics::default());
        assert!(snapshot.tasks[0].result.is_none());
        assert!(snapshot.tasks[0].history.is_empty());
        execute(&mut orchestrator, task_id, execution.clone()).await;
        let finished = events.try_recv().unwrap();
        assert_eq!(finished.metadata.seq, started.metadata.seq + 1);
        let EventData::OperationFinished(OperationFinished {
            context: actual,
            report,
        }) = finished.data
        else {
            panic!("expected operation finish");
        };
        assert_eq!(actual, context);
        assert_eq!(
            serde_json::to_value(*report).unwrap(),
            serde_json::to_value(execution.report).unwrap()
        );
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(transition)
            if transition.current == TaskStatus::Completed)
        );
    }

    #[tokio::test]
    async fn retained_operations_keep_original_context_after_updates_and_new_starts() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let first = execution(100, true);
        execute(&mut orchestrator, task_id, first.clone()).await;
        orchestrator.stop(task_id).await.unwrap();
        orchestrator
            .update(task_id, set_config("replacement"))
            .await
            .unwrap();
        orchestrator
            .start(task_id, RunLimit::Unlimited)
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        let generation = orchestrator.entries[&task_id].generation;
        let revision = orchestrator.entries[&task_id].task.revision();
        assert_ne!(generation, first.context.generation);
        assert_ne!(revision, first.context.revision);
        assert_eq!(
            orchestrator.snapshot().tasks[0]
                .result
                .as_ref()
                .unwrap()
                .context,
            first.context
        );
        let get = execution(200, true);
        let crate::monitor::OperationCompletion::Returned { detail } = get.report.completion else {
            unreachable!()
        };
        let ScenarioOutcome::SnmpGet(report) = *detail else {
            unreachable!()
        };
        let second = ScenarioExecution::new(
            crate::monitor::OperationContext::new(
                crate::monitor::OperationId::new(0),
                generation,
                revision,
            ),
            OperationReport::from_poll(
                PollReport {
                    started_at: get.report.started_at,
                    finished_at: get.report.finished_at,
                    elapsed: get.report.elapsed,
                    config: report.config,
                    attempts: report.attempts,
                    outcome: PollOutcome::Succeeded(()),
                },
                ScenarioOutcome::SnmpSet,
            ),
        );
        orchestrator
            .on_report(ScenarioReport::RunFinished {
                task_id,
                generation,
                execution: second.clone(),
                limit_reached: false,
            })
            .await;
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.result.as_ref().unwrap().context, second.context);
        assert_eq!(task.history[0].context, first.context);
        assert_eq!(task.metrics.executions, 1);
        let mut last = None;
        while let Ok(event) = events.try_recv() {
            last = Some(event);
        }
        let last = last.unwrap();
        assert_eq!(last.metadata.revision, revision);
        assert!(
            matches!(last.data, EventData::OperationFinished(OperationFinished { context, .. }) if context == second.context)
        );
        orchestrator.stop(task_id).await.unwrap();
        orchestrator
            .start(task_id, RunLimit::Unlimited)
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics, ExecutionMetrics::default());
        assert_eq!(snapshot.tasks[0].result.as_ref().unwrap().context, second.context);
        assert_eq!(snapshot.tasks[0].history[0].context, first.context);
    }

    #[tokio::test]
    async fn operation_starts_from_retired_generations_are_ignored() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let config = AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap();
        let retired = crate::monitor::OperationContext::new(
            crate::monitor::OperationId::new(0),
            Generation::first().next(),
            SpecRevision::new(1),
        );
        orchestrator
            .on_report(ScenarioReport::OperationStarted {
                task_id,
                context: retired,
                purpose: "Read state".to_string(),
                started_at: Utc::now(),
                config,
            })
            .await;
        assert!(events.try_recv().is_err());
        orchestrator.stop(task_id).await.unwrap();
        events.try_recv().unwrap();
        orchestrator
            .on_report(ScenarioReport::OperationStarted {
                task_id,
                context: execution(100, true).context,
                purpose: "Read state".to_string(),
                started_at: Utc::now(),
                config,
            })
            .await;
        assert!(events.try_recv().is_err());
        assert!(orchestrator.snapshot().tasks[0].result.is_none());
    }

    #[tokio::test]
    async fn no_response_is_an_execution_and_does_not_restart_the_scenario() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let first = execution(100, false);
        execute(&mut orchestrator, task_id, first.clone()).await;

        let event = events.try_recv().unwrap();
        let EventData::OperationFinished(OperationFinished { context, report }) = event.data else {
            panic!("expected an execution event");
        };
        assert_eq!(context, first.context);
        assert!(matches!(
            report.completion,
            crate::monitor::OperationCompletion::Returned { .. }
        ));
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.status, TaskStatus::Active);
        assert_eq!(task.metrics.executions, 1);
        assert_eq!(task.metrics.failed, 1);
        assert_eq!(task.metrics.current_duration, first.elapsed());
        assert_eq!(task.metrics.consecutive_failures, 1);
        assert_eq!(task.result.as_ref().unwrap().finished_at(), first.finished_at());
        assert!(orchestrator.entries[&task_id].scenario.is_some());

        execute(&mut orchestrator, task_id, self::execution(200, true)).await;
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.metrics.executions, 2);
        assert_eq!(task.metrics.successful, 1);
        assert_eq!(task.metrics.failed, 1);
        assert_eq!(task.metrics.consecutive_failures, 0);
        assert_eq!(task.history.len(), 1);
        assert_eq!(task.history[0].finished_at(), first.finished_at());
    }

    #[tokio::test]
    async fn final_execution_is_published_before_task_completion() {
        let (mut orchestrator, mut events, task_id) = active_task(1);
        execute(&mut orchestrator, task_id, execution(100, true)).await;

        let executed = events.try_recv().unwrap();
        let completed = events.try_recv().unwrap();
        assert!(matches!(executed.data, EventData::OperationFinished(_)));
        assert!(
            matches!(completed.data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Completed)
        );
        assert_eq!(completed.metadata.seq, executed.metadata.seq + 1);
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Completed);
        assert_eq!(snapshot.tasks[0].run_limit, Some(RunLimit::Runs(std::num::NonZeroU64::MIN)));
        assert_eq!(snapshot.tasks[0].completed_runs, 1);
        assert_eq!(snapshot.tasks[0].metrics.executions, 1);
        assert!(snapshot.tasks[0].result.is_some());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
    }

    #[tokio::test]
    async fn limit_counts_a_full_failed_request_once_including_all_retries() {
        let (mut orchestrator, mut events, task_id) = active_task(2);
        execute(&mut orchestrator, task_id, execution(100, false)).await;

        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics.executions, 1);
        assert_eq!(snapshot.tasks[0].metrics.failed, 1);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Active);
        assert!(matches!(events.try_recv().unwrap().data, EventData::OperationFinished(_)));
        assert!(events.try_recv().is_err());

        execute(&mut orchestrator, task_id, execution(200, false)).await;

        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics.executions, 2);
        assert_eq!(snapshot.tasks[0].metrics.failed, 2);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Completed);
        assert!(matches!(events.try_recv().unwrap().data, EventData::OperationFinished(_)));
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Completed)
        );
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn execution_from_another_generation_is_ignored() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(ScenarioReport::RunFinished {
                task_id,
                generation: Generation::first().next(),
                execution: execution(100, true),
                limit_reached: false,
            })
            .await;

        assert!(events.try_recv().is_err());
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics, ExecutionMetrics::default());
        assert!(snapshot.tasks[0].result.is_none());
    }

    #[tokio::test]
    async fn execution_error_preserves_the_report_without_changing_limit_accounting() {
        let mut failed = execution(200, false);
        let crate::monitor::OperationCompletion::Returned { detail } =
            &mut failed.report.completion
        else {
            unreachable!()
        };
        let ScenarioOutcome::SnmpGet(report) = detail.as_mut() else {
            unreachable!()
        };
        report.outcome = PollOutcome::NonRetryableFailure;
        report.attempts[0].outcome = AttemptOutcome::Failed {
            error: SnmpError::InvalidOid("invalid request".to_string()),
            retryable: false,
        };
        let (mut orchestrator, mut events, task_id) = active_task(2);
        execute(&mut orchestrator, task_id, execution(100, false)).await;
        let metrics = orchestrator.snapshot().tasks[0].metrics;
        events.try_recv().unwrap();
        orchestrator
            .on_report(ScenarioReport::ExecutionFailed {
                task_id,
                generation: Generation::first(),
                error: ScenarioExecutionError::Snmp(SnmpError::InvalidOid(
                    "invalid request".to_string(),
                )),
                execution: failed.clone(),
            })
            .await;

        let EventData::OperationFinished(OperationFinished { context, report }) =
            events.try_recv().unwrap().data
        else {
            panic!("expected a failed execution with its report");
        };
        assert_eq!(context, failed.context);
        assert_eq!(
            serde_json::to_value(*report).unwrap(),
            serde_json::to_value(&failed.report).unwrap()
        );
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.status, TaskStatus::Restarting);
        let recovered = events.try_recv().unwrap();
        let EventData::TaskStateChanged(transition) = recovered.data else {
            panic!("expected the recovery transition after the operation report");
        };
        assert!(matches!(&transition.cause,
            TaskTransitionCause::Failure(failure)
                if matches!(failure.as_ref(), ScenarioFailure::Execution(_))));
        assert_eq!(
            serde_json::to_value(&transition).unwrap(),
            serde_json::to_value(task.last_transition.as_ref().unwrap()).unwrap()
        );
        assert_eq!(task.metrics.executions, 1);
        assert_eq!(task.metrics, metrics);
        assert_eq!(orchestrator.entries[&task_id].task.completed_runs(), 1);
        assert_eq!(task.metrics.consecutive_failures, 1);
        assert!(matches!(&task.last_failure,
            Some(ScenarioFailure::Execution(ScenarioExecutionError::Snmp(
                SnmpError::InvalidOid(reason)
            ))) if reason == "invalid request"));
        assert_eq!(task.history.len(), 1);
        assert_eq!(task.history[0].finished_at(), DateTime::from_timestamp(100, 0).unwrap());
        assert_eq!(task.result.as_ref().unwrap().finished_at(), failed.finished_at());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
    }

    #[tokio::test]
    async fn text_progress_preserves_lifecycle_and_source_time_and_clears_on_readiness() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .transition(task_id, TaskStatus::Preparing, TaskTransitionCause::InitialStart, None)
            .await;
        let transition = take_transition(&orchestrator, &mut events, task_id);
        let at = Utc::now();
        for (index, message) in ["Loading scenario data", "Reading SCN", "Reading SCN"]
            .into_iter()
            .enumerate()
        {
            let progress = TaskProgress {
                at: at + chrono::Duration::seconds(i64::try_from(index).unwrap()),
                message: message.to_string(),
            };
            orchestrator
                .on_report(ScenarioReport::Progress {
                    task_id,
                    generation: Generation::first(),
                    progress: progress.clone(),
                })
                .await;
            let snapshot = orchestrator.snapshot();
            let task = &snapshot.tasks[0];
            assert_eq!(task.status, TaskStatus::Preparing);
            assert_eq!(task.progress.as_ref(), Some(&progress));
            assert_eq!(task.metrics.executions, 0);
            assert_eq!(
                serde_json::to_value(task.last_transition.as_ref().unwrap()).unwrap(),
                serde_json::to_value(&transition).unwrap()
            );
            let event = events.try_recv().unwrap();
            assert_eq!(event.metadata.seq, u64::try_from(index + 1).unwrap());
            assert_eq!(event.metadata.generation, Generation::first());
            assert_eq!(event.metadata.revision, SpecRevision::new(1));
            assert!(matches!(event.data, EventData::Preparation(reported) if reported == progress));
            assert!(events.try_recv().is_err());
        }
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: Generation::first(),
            })
            .await;
        let ready = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(ready.current, TaskStatus::Active);
        assert!(orchestrator.snapshot().tasks[0].progress.is_none());
        orchestrator
            .on_report(ScenarioReport::Progress {
                task_id,
                generation: Generation::first(),
                progress: TaskProgress {
                    at,
                    message: "Late preparation message".to_string(),
                },
            })
            .await;
        assert!(orchestrator.snapshot().tasks[0].progress.is_none());
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn rejected_update_preserves_preparation_progress() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator.entries.get_mut(&task_id).unwrap().state = TaskStatus::Preparing;
        let retired = Generation::first();
        let progress = TaskProgress {
            at: Utc::now(),
            message: "Old configuration".to_string(),
        };
        orchestrator
            .on_report(ScenarioReport::Progress {
                task_id,
                generation: retired,
                progress: progress.clone(),
            })
            .await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::Preparation(_)));
        assert!(matches!(
            orchestrator.update(task_id, set_config("replacement")).await,
            Err(MonitorCommandError::TaskUpdateWhileRunning { task_id: rejected, status: TaskStatus::Preparing })
                if rejected == task_id
        ));
        let task = orchestrator.snapshot().tasks.remove(0);
        assert_eq!(task.status, TaskStatus::Preparing);
        assert_eq!(task.progress.as_ref(), Some(&progress));
        assert_eq!(task.configuration.revision, SpecRevision::new(1));
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn preparation_failure_and_stop_clear_current_progress() {
        for fail in [false, true] {
            let (mut orchestrator, mut events, task_id) = active_task(0);
            orchestrator.entries.get_mut(&task_id).unwrap().state = TaskStatus::Preparing;
            let progress = TaskProgress {
                at: Utc::now(),
                message: "Reading SCN".to_string(),
            };
            orchestrator
                .on_report(ScenarioReport::Progress {
                    task_id,
                    generation: Generation::first(),
                    progress,
                })
                .await;
            events.try_recv().unwrap();
            if fail {
                orchestrator
                    .on_report(ScenarioReport::BuildFailed {
                        task_id,
                        generation: Generation::first(),
                        error: ScenarioBuildError::Timeout,
                    })
                    .await;
            } else {
                orchestrator.stop(task_id).await.unwrap();
            }
            let task = orchestrator.snapshot().tasks.remove(0);
            assert!(task.progress.is_none());
            assert_eq!(
                task.status,
                if fail {
                    TaskStatus::Restarting
                } else {
                    TaskStatus::Stopped
                }
            );
            assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(_)));
        }
    }

    #[tokio::test]
    async fn build_failures_preserve_their_type_and_restart_the_scenario() {
        for error in [
            ScenarioBuildError::Timeout,
            ScenarioBuildError::SnmpClientCreate,
            ScenarioBuildError::Other("build timeout".to_string()),
        ] {
            let (mut orchestrator, mut events, task_id) = active_task(0);
            let generation = orchestrator.entries[&task_id].generation;
            orchestrator
                .on_report(ScenarioReport::BuildFailed {
                    task_id,
                    generation,
                    error: error.clone(),
                })
                .await;

            let event = events.try_recv().unwrap();
            assert_eq!(event.metadata.task_id, task_id);
            assert_eq!(event.metadata.generation, generation);
            let EventData::TaskStateChanged(transition) = event.data else {
                panic!("expected a recovery transition");
            };
            assert!(matches!(&transition.cause,
                TaskTransitionCause::Failure(failure)
                if matches!(failure.as_ref(), ScenarioFailure::Preparation(reported) if reported == &error)));
            assert_eq!(transition.current, TaskStatus::Restarting);
            assert_eq!(transition.restart.unwrap().attempt, 1);
            let snapshot = orchestrator.snapshot();
            assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
            assert!(matches!(&snapshot.tasks[0].last_failure,
                Some(ScenarioFailure::Preparation(reported)) if reported == &error));
            assert_eq!(
                snapshot.tasks[0].last_failure.as_ref().unwrap().to_string(),
                error.to_string()
            );
            assert_eq!(snapshot.tasks[0].metrics.executions, 0);
            assert!(snapshot.tasks[0].result.is_none());
            assert_eq!(orchestrator.entries[&task_id].attempts, 1);
            assert!(orchestrator.entries[&task_id].scenario.is_none());
            assert!(events.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn scenario_panics_preserve_the_public_event_contract() {
        for (stage, label) in [
            (PanicStage::Build, "build"),
            (PanicStage::Execution, "execution"),
            (PanicStage::Runtime, "scenario"),
        ] {
            let (mut orchestrator, mut events, task_id) = active_task(0);
            let generation = orchestrator.entries[&task_id].generation;
            let reason = "panic".to_string();
            let report = match stage {
                PanicStage::Build => ScenarioReport::BuildPanicked {
                    task_id,
                    generation,
                    reason,
                },
                PanicStage::Execution => ScenarioReport::ExecutionPanicked {
                    task_id,
                    generation,
                    reason,
                },
                PanicStage::Runtime => ScenarioReport::Panicked {
                    task_id,
                    generation,
                    reason,
                },
            };
            orchestrator.on_report(report).await;
            let event = events.try_recv().unwrap();
            assert_eq!(event.metadata.task_id, task_id);
            assert_eq!(event.metadata.generation, generation);
            let EventData::TaskStateChanged(transition) = event.data else {
                panic!("expected a panic recovery transition");
            };
            assert!(matches!(&transition.cause,
                TaskTransitionCause::Failure(failure)
                if matches!(failure.as_ref(), ScenarioFailure::Panicked { stage: actual, reason }
                    if *actual == stage && reason == "panic")));
            assert_eq!(transition.restart.unwrap().attempt, 1);
            let snapshot = orchestrator.snapshot();
            assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
            assert_eq!(snapshot.tasks[0].metrics.executions, 0);
            assert_eq!(
                snapshot.tasks[0].last_failure.as_ref().unwrap().to_string(),
                format!("{label}: panic")
            );
            assert!(matches!(&snapshot.tasks[0].last_failure,
                Some(ScenarioFailure::Panicked { stage: actual, reason })
                    if *actual == stage && reason == "panic"));
        }
    }

    #[tokio::test]
    async fn start_uses_saved_scenario_and_resets_run_metrics() {
        let (mut orchestrator, mut events, task_id) = active_task(2);
        execute(&mut orchestrator, task_id, execution(100, true)).await;
        orchestrator.stop(task_id).await.unwrap();
        orchestrator
            .update(task_id, set_config("fresh"))
            .await
            .unwrap();
        orchestrator
            .start(task_id, RunLimit::Unlimited)
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(matches!(events.try_recv().unwrap().data, EventData::OperationFinished(_)));
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Stopped)
        );
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::TaskConfigurationChanged(_)
        ));
        let starting = events.try_recv().unwrap();
        assert!(
            matches!(starting.data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Preparing)
        );
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.configuration.description.name, "fresh");
        assert_eq!(task.run_limit, Some(RunLimit::Unlimited));
        assert_eq!(task.status, TaskStatus::Preparing);
        assert_eq!(task.metrics, ExecutionMetrics::default());
        assert!(matches!(&task.configuration.description.scenario,
                crate::monitor::ScenarioDescription::SnmpSet { target, schedule, .. }
                    if *target == "127.0.0.2:1161".parse().unwrap()
                        && schedule.duration() == Duration::from_secs(2)));
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: starting.metadata.generation,
            })
            .await;
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Active)
        );
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
    }

    #[tokio::test]
    async fn pause_and_resume_wait_for_acknowledgements_and_stop_rejects_late_reports() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let generation = orchestrator.entries[&task_id].generation;
        let scenario = orchestrator.entries[&task_id].scenario.as_ref().unwrap();
        let mut mode = scenario.mode_tx.subscribe();
        let abort = scenario.abort.clone();

        orchestrator.pause(task_id).await.unwrap();
        assert!(mode.has_changed().unwrap());
        assert_eq!(*mode.borrow_and_update(), RunMode::Paused);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation,
            })
            .await;
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Paused)
        );

        orchestrator.pause(task_id).await.unwrap();
        assert!(!mode.has_changed().unwrap());
        assert!(events.try_recv().is_err());
        orchestrator.resume(task_id).await.unwrap();
        assert!(mode.has_changed().unwrap());
        assert_eq!(*mode.borrow_and_update(), RunMode::Running);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Resumed {
                task_id,
                generation,
            })
            .await;
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Active)
        );

        orchestrator.resume(task_id).await.unwrap();
        assert!(!mode.has_changed().unwrap());
        assert!(events.try_recv().is_err());
        orchestrator.stop(task_id).await.unwrap();
        assert!(abort.is_finished());
        assert!(mode.changed().await.is_err());
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Stopped)
        );
        orchestrator
            .on_report(ScenarioReport::RunFinished {
                task_id,
                generation,
                execution: execution(100, true),
                limit_reached: false,
            })
            .await;
        orchestrator
            .on_report(ScenarioReport::Resumed {
                task_id,
                generation,
            })
            .await;
        orchestrator.maybe_respawn(task_id, generation).await;
        orchestrator.stop(task_id).await.unwrap();
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Stopped);
        assert_eq!(orchestrator.snapshot().tasks[0].metrics, ExecutionMetrics::default());
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn latest_mode_replaces_unconfirmed_pause_and_resume_requests() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let scenario = orchestrator.entries[&task_id].scenario.as_ref().unwrap();
        let mut mode = scenario.mode_tx.subscribe();

        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..16 {
                orchestrator.pause(task_id).await.unwrap();
            }
            assert_eq!(*mode.borrow(), RunMode::Paused);
            assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
            orchestrator.resume(task_id).await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(*mode.borrow_and_update(), RunMode::Running);
        assert!(events.try_recv().is_err());

        orchestrator.pause(task_id).await.unwrap();
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation: Generation::first(),
            })
            .await;
        assert_eq!(*mode.borrow_and_update(), RunMode::Paused);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);

        orchestrator.resume(task_id).await.unwrap();
        assert_eq!(*mode.borrow(), RunMode::Running);
        orchestrator.pause(task_id).await.unwrap();
        assert_eq!(*mode.borrow_and_update(), RunMode::Paused);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
    }

    #[tokio::test]
    async fn rejected_update_keeps_active_scenario_and_accounting() {
        let (mut orchestrator, mut events, task_id) = active_task(2);
        execute(&mut orchestrator, task_id, execution(100, true)).await;
        events.try_recv().unwrap();
        let generation = orchestrator.entries[&task_id].generation;
        let before = orchestrator.snapshot().tasks.remove(0);

        assert!(matches!(
            orchestrator.update(task_id, set_config("replacement")).await,
            Err(MonitorCommandError::TaskUpdateWhileRunning { task_id: rejected, status: TaskStatus::Active })
                if rejected == task_id
        ));

        let task = orchestrator.snapshot().tasks.remove(0);
        assert_eq!(task.configuration.revision, before.configuration.revision);
        assert_eq!(task.configuration.description.name, before.configuration.description.name);
        assert_eq!(task.metrics, before.metrics);
        assert_eq!(task.completed_runs, 1);
        assert_eq!(orchestrator.entries[&task_id].generation, generation);
        assert!(events.try_recv().is_err());

        execute(&mut orchestrator, task_id, execution(200, true)).await;
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Completed);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_restart_timer_cannot_restart_a_newer_generation() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let previous_generation = orchestrator.entries[&task_id].generation;
        orchestrator.entries[&task_id]
            .scenario
            .as_ref()
            .unwrap()
            .abort
            .abort();
        orchestrator
            .on_report(ScenarioReport::BuildFailed {
                task_id,
                generation: previous_generation,
                error: ScenarioBuildError::Other("unavailable".to_string()),
            })
            .await;
        events.try_recv().unwrap();
        let stale_restart =
            tokio::time::timeout(Duration::from_secs(2), orchestrator.cmd_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(
            matches!(stale_restart, Command::Respawn { generation, .. } if generation == previous_generation)
        );

        assert!(matches!(
            orchestrator.update(task_id, set_config("replacement")).await,
            Err(MonitorCommandError::TaskUpdateWhileRunning { task_id: rejected, status: TaskStatus::Restarting })
                if rejected == task_id
        ));
        assert!(events.try_recv().is_err());
        orchestrator.stop(task_id).await.unwrap();
        assert!(matches!(events.try_recv().unwrap().data,
            EventData::TaskStateChanged(transition) if transition.current == TaskStatus::Stopped));
        orchestrator
            .maybe_respawn(task_id, previous_generation)
            .await;
        assert!(events.try_recv().is_err());

        orchestrator
            .update(task_id, set_config("latest"))
            .await
            .unwrap();
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::TaskConfigurationChanged(_)
        ));
        orchestrator
            .start(task_id, RunLimit::Unlimited)
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        let starting = events.try_recv().unwrap();
        assert!(
            matches!(starting.data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Preparing)
        );
        let generation = starting.metadata.generation;
        assert_eq!(generation, previous_generation.next());
        fail_scenario(&mut orchestrator, task_id).await;
        assert!(matches!(events.try_recv().unwrap().data,
            EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Restarting));
        assert_eq!(orchestrator.entries[&task_id].attempts, 1);

        assert!(orchestrator.on_command(stale_restart).await);
        assert_eq!(orchestrator.entries[&task_id].generation, generation);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Restarting);
        assert!(orchestrator.entries[&task_id].scenario.is_none());
        assert!(events.try_recv().is_err());
        let current_restart =
            tokio::time::timeout(Duration::from_secs(2), orchestrator.cmd_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(
            matches!(current_restart, Command::Respawn { generation: actual, .. } if actual == generation)
        );
        assert!(orchestrator.on_command(current_restart).await);
        control_scenario(&mut orchestrator, task_id);
        let restarting = events.try_recv().unwrap();
        assert!(
            matches!(restarting.data, EventData::TaskStateChanged(ref transition) if matches!(transition.cause, TaskTransitionCause::RestartDue { attempt: 1 }))
        );
        assert_eq!(restarting.metadata.generation, generation.next());
        assert_eq!(restarting.metadata.revision, SpecRevision::new(2));
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].configuration.description.name, "latest");
        assert!(matches!(&snapshot.tasks[0].configuration.description.scenario,
                crate::monitor::ScenarioDescription::SnmpSet { schedule, .. }
                    if schedule.duration() == Duration::from_secs(2)));
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: restarting.metadata.generation,
            })
            .await;
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Active)
        );
        assert_eq!(orchestrator.entries[&task_id].attempts, 0);
    }

    #[tokio::test]
    async fn pausing_backoff_blocks_respawn_and_resume_resets_retry_attempts() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .entries
            .get_mut(&task_id)
            .unwrap()
            .task
            .update_config(set_config("resumed"));
        let generation = orchestrator.entries[&task_id].generation;
        orchestrator.entries[&task_id]
            .scenario
            .as_ref()
            .unwrap()
            .abort
            .abort();
        orchestrator
            .on_report(ScenarioReport::BuildFailed {
                task_id,
                generation,
                error: ScenarioBuildError::Other("unavailable".to_string()),
            })
            .await;
        events.try_recv().unwrap();
        orchestrator.pause(task_id).await.unwrap();
        assert!(
            matches!(events.try_recv().unwrap().data, EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Paused)
        );
        orchestrator.maybe_respawn(task_id, generation).await;
        assert!(events.try_recv().is_err());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);

        orchestrator.resume(task_id).await.unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::TaskStateChanged(ref transition) if transition.current == TaskStatus::Preparing
        ));
        assert_eq!(orchestrator.entries[&task_id].attempts, 0);
        fail_scenario(&mut orchestrator, task_id).await;
        assert_eq!(orchestrator.entries[&task_id].attempts, 1);
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::TaskStateChanged(ref transition)
                if transition.current == TaskStatus::Restarting
                    && transition.restart.unwrap().attempt == 1
        ));
    }

    fn take_transition(
        orchestrator: &Orchestrator,
        events: &mut broadcast::Receiver<MonitorEvent>,
        task_id: TaskId,
    ) -> TaskTransition {
        let event = events.try_recv().unwrap();
        assert_eq!(event.metadata.task_id, task_id);
        let EventData::TaskStateChanged(transition) = event.data else {
            panic!("expected a state transition");
        };
        let snapshot = orchestrator.snapshot();
        let view = snapshot
            .tasks
            .iter()
            .find(|view| view.id == task_id)
            .unwrap();
        assert_eq!(view.status, transition.current);
        assert_eq!(
            serde_json::to_value(&transition).unwrap(),
            serde_json::to_value(view.last_transition.as_ref().unwrap()).unwrap()
        );
        *transition
    }

    #[tokio::test]
    async fn transition_readiness_records_reason_and_clears_previous_failure() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let entry = orchestrator.entries.get_mut(&task_id).unwrap();
        entry.state = TaskStatus::Preparing;
        entry.attempts = 2;
        entry.progress = Some(TaskProgress {
            at: Utc::now(),
            message: "Finishing preparation".to_string(),
        });
        entry.last_failure = Some(ScenarioFailure::Preparation(ScenarioBuildError::Other(
            "previous failure".to_string(),
        )));
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: Generation::first(),
            })
            .await;
        let ready = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(ready.previous, TaskStatus::Preparing);
        assert_eq!(ready.current, TaskStatus::Active);
        assert!(matches!(ready.cause, TaskTransitionCause::ScenarioReady));
        assert!(ready.restart.is_none());
        assert_eq!(orchestrator.entries[&task_id].attempts, 0);
        assert!(orchestrator.snapshot().tasks[0].last_failure.is_none());
        assert!(orchestrator.snapshot().tasks[0].progress.is_none());
    }

    #[tokio::test]
    async fn typed_failure_survives_restart_preparation_until_readiness() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        execute(&mut orchestrator, task_id, execution(100, false)).await;
        events.try_recv().unwrap();
        fail_scenario(&mut orchestrator, task_id).await;
        let recovery = take_transition(&orchestrator, &mut events, task_id);
        let expected = orchestrator.snapshot().tasks[0]
            .last_failure
            .clone()
            .unwrap();

        orchestrator
            .maybe_respawn(task_id, Generation::first())
            .await;
        control_scenario(&mut orchestrator, task_id);
        let preparing = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(preparing.current, TaskStatus::Preparing);
        assert!(matches!(preparing.cause, TaskTransitionCause::RestartDue { .. }));
        let snapshot = orchestrator.snapshot();
        let retained = snapshot.tasks[0].last_failure.as_ref().unwrap();
        assert_eq!(
            serde_json::to_value(retained).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert!(matches!(recovery.cause, TaskTransitionCause::Failure(_)));
        assert_eq!(snapshot.tasks[0].metrics.consecutive_failures, 1);

        let generation = orchestrator.entries[&task_id].generation;
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation,
            })
            .await;
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Active);
        assert!(snapshot.tasks[0].last_failure.is_none());
        assert_eq!(snapshot.tasks[0].metrics.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn rejected_update_retains_paused_transition_and_scenario() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation: Generation::first(),
            })
            .await;
        let paused = take_transition(&orchestrator, &mut events, task_id);
        assert!(matches!(
            orchestrator.update(task_id, set_config("replacement")).await,
            Err(MonitorCommandError::TaskUpdateWhileRunning { task_id: rejected, status: TaskStatus::Paused })
                if rejected == task_id
        ));
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Paused);
        assert_eq!(snapshot.tasks[0].configuration.revision, SpecRevision::new(1));
        assert_eq!(
            serde_json::to_value(snapshot.tasks[0].last_transition.as_ref().unwrap()).unwrap(),
            serde_json::to_value(paused).unwrap()
        );
        assert!(events.try_recv().is_err());
        assert!(orchestrator.entries[&task_id].scenario.is_some());
    }

    #[tokio::test]
    async fn transition_pause_resume_wait_for_acknowledgements_and_ignore_duplicates() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let generation = Generation::first();
        let mut mode = orchestrator.entries[&task_id]
            .scenario
            .as_ref()
            .unwrap()
            .mode_tx
            .subscribe();

        orchestrator.pause(task_id).await.unwrap();
        assert!(mode.has_changed().unwrap());
        assert_eq!(*mode.borrow_and_update(), RunMode::Paused);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation,
            })
            .await;
        let paused = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(paused.previous, TaskStatus::Active);
        assert!(matches!(paused.cause, TaskTransitionCause::Command(TaskCommandKind::Pause)));
        assert!(paused.restart.is_none());

        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation,
            })
            .await;
        orchestrator.pause(task_id).await.unwrap();
        assert!(!mode.has_changed().unwrap());
        assert!(events.try_recv().is_err());
        assert_eq!(
            serde_json::to_value(
                orchestrator.snapshot().tasks[0]
                    .last_transition
                    .as_ref()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&paused).unwrap()
        );

        orchestrator.resume(task_id).await.unwrap();
        assert!(mode.has_changed().unwrap());
        assert_eq!(*mode.borrow_and_update(), RunMode::Running);
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Resumed {
                task_id,
                generation,
            })
            .await;
        let resumed = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(resumed.previous, TaskStatus::Paused);
        assert!(matches!(resumed.cause, TaskTransitionCause::Command(TaskCommandKind::Resume)));
        orchestrator
            .on_report(ScenarioReport::Resumed {
                task_id,
                generation,
            })
            .await;
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn transition_recovery_plan_and_typed_error_survive_until_pause() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let error =
            ScenarioBuildError::Snmp(SnmpError::InvalidOid("invalid definition".to_string()));
        let before = Utc::now();
        orchestrator
            .on_report(ScenarioReport::BuildFailed {
                task_id,
                generation: Generation::first(),
                error: error.clone(),
            })
            .await;
        let recovered = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(recovered.previous, TaskStatus::Active);
        assert!(matches!(&recovered.cause, TaskTransitionCause::Failure(failure)
            if matches!(failure.as_ref(), ScenarioFailure::Preparation(reported) if reported == &error)));
        let plan = recovered.restart.unwrap();
        assert_eq!(plan.attempt, 1);
        assert!(plan.scheduled_at >= before + Duration::from_secs(1));
        assert!(plan.scheduled_at <= Utc::now() + Duration::from_secs(1));
        assert!(matches!(&orchestrator.snapshot().tasks[0].last_failure,
            Some(ScenarioFailure::Preparation(reported)) if reported == &error));

        orchestrator.pause(task_id).await.unwrap();
        let paused = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(paused.previous, TaskStatus::Restarting);
        assert!(paused.restart.is_none());
        orchestrator
            .maybe_respawn(task_id, Generation::first())
            .await;
        assert!(events.try_recv().is_err());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
        orchestrator.stop(task_id).await.unwrap();
        let stopped = take_transition(&orchestrator, &mut events, task_id);
        assert!(matches!(stopped.cause, TaskTransitionCause::Command(TaskCommandKind::Stop)));
        orchestrator.stop(task_id).await.unwrap();
        assert!(events.try_recv().is_err());
        assert!(matches!(&orchestrator.snapshot().tasks[0].last_failure,
            Some(ScenarioFailure::Preparation(reported)) if reported == &error));
        orchestrator
            .start(task_id, RunLimit::Unlimited)
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(orchestrator.snapshot().tasks[0].last_failure.is_none());
    }

    #[tokio::test]
    async fn transition_limit_follows_result_and_preserves_accounting() {
        let (mut orchestrator, mut events, task_id) = active_task(1);
        execute(&mut orchestrator, task_id, execution(100, false)).await;
        let result = events.try_recv().unwrap();
        assert!(matches!(result.data, EventData::OperationFinished(_)));
        let completed = take_transition(&orchestrator, &mut events, task_id);
        assert_eq!(completed.previous, TaskStatus::Active);
        assert_eq!(completed.current, TaskStatus::Completed);
        assert!(matches!(completed.cause, TaskTransitionCause::LimitReached { limit: 1 }));
        assert!(completed.restart.is_none());
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics.executions, 1);
        assert_eq!(snapshot.tasks[0].metrics.failed, 1);
        assert_eq!(snapshot.next_seq, result.metadata.seq + 2);
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn transition_shutdown_stops_live_tasks_without_transitioning_idle_tasks() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator.next_id = 2;
        let idle = orchestrator.register_task(set_config("idle"));
        assert!(orchestrator.entries[&idle].last_transition.is_none());
        let (reply, response) = oneshot::channel();
        assert!(!orchestrator.on_command(Command::Shutdown { reply }).await);
        response.await.unwrap();
        let event = events.try_recv().unwrap();
        assert_eq!(event.metadata.task_id, task_id);
        let EventData::TaskStateChanged(transition) = event.data else {
            panic!("expected the shutdown transition");
        };
        assert_eq!(transition.previous, TaskStatus::Active);
        assert_eq!(transition.current, TaskStatus::Stopped);
        assert!(matches!(transition.cause, TaskTransitionCause::Shutdown));
        assert!(transition.restart.is_none());
        assert!(orchestrator.entries.is_empty());
        assert!(events.try_recv().is_err());
    }
}
