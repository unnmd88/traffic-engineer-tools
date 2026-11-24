//! An alternative monitor runtime with one FIFO for commands, scenario updates and exits.

mod entry;

use std::collections::BTreeMap;

use chrono::Utc;
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tracing::Instrument;

use super::{
    command::{Command, OrchestratorHandle},
    event_output::{log_event, log_writer},
};
use crate::monitor::{
    AppConfig, ApplicationId, EventData, EventMeta, Generation, InitialTask, MonitorCommandError,
    MonitorEvent, MonitorSnapshot, MonitorSubscription, OperationFinished, RestartPlan,
    RestartPolicy, RunLimit, ScenarioExecution, ScenarioFailure, TaskCommandKind, TaskConfig,
    TaskDescription, TaskId, TaskStatus, TaskTransitionCause,
    scenario::{
        RunBudget, RunMode, ScenarioChannels, ScenarioContext, ScenarioExit, ScenarioUpdate,
        UpdateData, execute,
    },
};
use entry::{Entry, Execution, LiveRun};

pub(super) enum Message {
    Command(Box<Command>),
    Update(ScenarioUpdate),
    Exited(ScenarioContext, ScenarioExit),
}

impl From<Command> for Message {
    fn from(command: Command) -> Self {
        Self::Command(Box::new(command))
    }
}

impl From<ScenarioUpdate> for Message {
    fn from(update: ScenarioUpdate) -> Self {
        Self::Update(update)
    }
}

pub(super) struct OrderedOrchestrator {
    id: ApplicationId,
    next_id: u64,
    seq: u64,
    entries: BTreeMap<TaskId, Entry>,
    initial: Vec<(TaskId, RunLimit)>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    events: broadcast::Sender<MonitorEvent>,
    history: Option<mpsc::Sender<MonitorEvent>>,
    workers: JoinSet<()>,
    restart: RestartPolicy,
    shutdown: Option<oneshot::Sender<()>>,
}

impl OrderedOrchestrator {
    pub(super) fn new(
        id: ApplicationId,
        config: AppConfig,
        tasks: Vec<InitialTask>,
    ) -> (Self, OrchestratorHandle, Vec<TaskId>) {
        let (tx, rx) = mpsc::channel(256);
        let (events, _) = broadcast::channel(1024);
        let handle = OrchestratorHandle::ordered(tx.clone(), events.downgrade());
        let mut actor = Self {
            id,
            next_id: 1,
            seq: 0,
            entries: BTreeMap::new(),
            initial: Vec::new(),
            tx,
            rx,
            events,
            history: config.history_sink,
            workers: JoinSet::new(),
            restart: config.restart,
            shutdown: None,
        };
        for task in tasks {
            let id = actor.register(task.config);
            actor.initial.push((id, task.limit));
        }
        let ids = actor.initial.iter().map(|(id, _)| *id).collect();
        (actor, handle, ids)
    }

    fn register(&mut self, config: TaskConfig) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        self.entries.insert(id, Entry::new(id, config));
        id
    }

    pub(super) fn snapshot(&self) -> MonitorSnapshot {
        MonitorSnapshot {
            application_id: self.id,
            next_seq: self.seq,
            tasks: self.entries.values().map(Entry::snapshot).collect(),
        }
    }

    #[tracing::instrument(name = "monitor", skip_all, fields(application_id = %self.id))]
    pub(super) async fn run(mut self) {
        let mut writer = JoinSet::new();
        if let Some(sink) = self.history.take() {
            let (tx, rx) = mpsc::channel(1024);
            self.history = Some(tx);
            writer.spawn(log_writer(rx, sink).in_current_span());
        }
        let initial = std::mem::take(&mut self.initial);
        for &(id, _) in &initial {
            self.registered(id).await;
        }
        for (id, limit) in initial {
            self.start(id, limit, TaskTransitionCause::InitialStart)
                .await
                .expect("registered initial task");
        }
        loop {
            tokio::select! {
                Some(message) = self.rx.recv() => self.handle_message(message).await,
                Some(joined) = self.workers.join_next() => {
                    if let Err(error) = joined && error.is_panic() {
                        std::panic::resume_unwind(error.into_panic());
                    }
                }
            }
            if self.shutdown.is_some()
                && self
                    .entries
                    .values()
                    .all(|entry| !matches!(entry.execution, Execution::Live(_)))
            {
                break;
            }
        }
        drop(self.rx);
        self.workers.shutdown().await;
        self.history.take();
        writer.join_all().await;
        if let Some(reply) = self.shutdown {
            let _ = reply.send(());
        }
    }

    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::Command(command) if self.shutdown.is_none() => self.command(*command).await,
            Message::Command(_) => {}
            Message::Update(update) => self.update_from_scenario(update).await,
            Message::Exited(context, exit) => self.exited(context, exit).await,
        }
    }

    async fn publish(&mut self, id: TaskId, data: EventData) {
        let entry = &self.entries[&id];
        let metadata = EventMeta {
            application_id: self.id,
            task_id: id,
            seq: self.seq,
            at: Utc::now(),
            generation: entry.generation.unwrap_or(Generation::first()),
            revision: entry.task.revision(),
        };
        self.seq += 1;
        let event = MonitorEvent { metadata, data };
        log_event(&event);
        if self.events.receiver_count() > 0 {
            let _ = self.events.send(event.clone());
        }
        if let Some(tx) = &self.history
            && tx.send(event).await.is_err()
        {
            self.history = None;
        }
    }

    async fn registered(&mut self, id: TaskId) {
        let description = TaskDescription::from(self.entries[&id].task.config());
        self.publish(id, EventData::TaskRegistered(Box::new(description)))
            .await;
    }

    async fn transition(
        &mut self,
        id: TaskId,
        status: TaskStatus,
        cause: TaskTransitionCause,
        plan: Option<RestartPlan>,
    ) {
        if let Some(event) = self
            .entries
            .get_mut(&id)
            .expect("registered task")
            .transition(status, cause, plan)
        {
            self.publish(id, event).await;
        }
    }

    fn entry(&mut self, id: TaskId) -> Result<&mut Entry, MonitorCommandError> {
        self.entries
            .get_mut(&id)
            .ok_or(MonitorCommandError::TaskNotFound(id))
    }

    async fn command(&mut self, command: Command) {
        if let Some(id) = task_id(&command)
            && let Some(entry) = self.entries.get_mut(&id)
            && let Execution::Live(run) = &mut entry.execution
            && (run.is_closing()
                || matches!(command, Command::StopTask { .. } | Command::RemoveTask { .. }))
        {
            run.cancel();
            run.pending.push(command);
            return;
        }
        match command {
            Command::AddTask { spec, reply } => {
                let id = self.register(spec);
                self.registered(id).await;
                let _ = reply.send(Ok(id));
            }
            Command::StartTask {
                task_id,
                limit,
                reply,
            } => {
                let _ = reply.send(
                    self.start(
                        task_id,
                        limit,
                        TaskTransitionCause::Command(TaskCommandKind::Start),
                    )
                    .await,
                );
            }
            Command::PauseTask { task_id, reply } => {
                let _ = reply.send(self.mode(task_id, RunMode::Paused).await);
            }
            Command::ResumeTask { task_id, reply } => {
                let _ = reply.send(self.mode(task_id, RunMode::Running).await);
            }
            Command::StopTask { task_id, reply } => {
                let _ = reply.send(
                    self.stop(task_id, TaskTransitionCause::Command(TaskCommandKind::Stop))
                        .await,
                );
            }
            Command::RemoveTask { task_id, reply } => {
                let result = if self.entries.contains_key(&task_id) {
                    self.publish(task_id, EventData::TaskRemoved).await;
                    self.entries
                        .remove(&task_id)
                        .expect("registered task")
                        .cancel_timer();
                    Ok(())
                } else {
                    Err(MonitorCommandError::TaskNotFound(task_id))
                };
                let _ = reply.send(result);
            }
            Command::UpdateTask {
                task_id,
                spec,
                reply,
            } => {
                let result = self.update_config(task_id, spec).await;
                let _ = reply.send(result);
            }
            Command::GetSnapshot { reply } => {
                let _ = reply.send(self.snapshot());
            }
            Command::SubscribeWithSnapshot { reply } => {
                let _ = reply.send(MonitorSubscription {
                    events: self.events.subscribe(),
                    snapshot: self.snapshot(),
                });
            }
            Command::Respawn {
                task_id,
                generation,
            } => {
                if self.entries.get(&task_id).is_some_and(|entry| {
                    entry.generation == Some(generation)
                        && matches!(entry.execution, Execution::Backoff(_))
                }) {
                    let attempt = self.entries[&task_id].attempts;
                    self.launch(task_id, TaskTransitionCause::RestartDue { attempt })
                        .await;
                }
            }
            Command::Shutdown { reply } => {
                self.shutdown = Some(reply);
                for id in self.entries.keys().copied().collect::<Vec<_>>() {
                    self.stop_for_shutdown(id).await;
                }
            }
        }
    }

    async fn stop_for_shutdown(&mut self, id: TaskId) {
        if let Some(entry) = self.entries.get_mut(&id) {
            if let Execution::Live(run) = &mut entry.execution {
                run.cancel();
            } else {
                self.stop(id, TaskTransitionCause::Shutdown)
                    .await
                    .expect("registered task");
            }
        }
    }

    async fn update_config(
        &mut self,
        id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), MonitorCommandError> {
        let entry = self.entry(id)?;
        if !entry.can_start() {
            return Err(MonitorCommandError::TaskUpdateWhileRunning {
                task_id: id,
                status: entry.status(),
            });
        }
        entry.task.update_config(spec);
        let description = TaskDescription::from(entry.task.config());
        self.publish(id, EventData::TaskConfigurationChanged(Box::new(description)))
            .await;
        Ok(())
    }

    async fn start(
        &mut self,
        id: TaskId,
        limit: RunLimit,
        cause: TaskTransitionCause,
    ) -> Result<(), MonitorCommandError> {
        let entry = self.entry(id)?;
        if !entry.can_start() {
            return if entry.task.run_limit() == Some(limit) {
                Ok(())
            } else {
                Err(MonitorCommandError::TaskAlreadyRunning(id))
            };
        }
        entry.task.begin_run(limit);
        entry.desired = RunMode::Running;
        entry.last_failure = None;
        entry.attempts = 0;
        self.launch(id, cause).await;
        Ok(())
    }

    async fn launch(&mut self, id: TaskId, cause: TaskTransitionCause) {
        let entry = self.entries.get_mut(&id).expect("registered task");
        let generation = entry
            .generation
            .map_or(Generation::first(), Generation::next);
        let context = ScenarioContext {
            task_id: id,
            generation,
            revision: entry.task.revision(),
        };
        let budget = RunBudget::new(
            entry.task.run_limit().expect("started task"),
            entry.task.completed_runs(),
        );
        let (mode, rx) = watch::channel(entry.desired);
        let spec = entry.task.config().scenario().clone();
        let channels = ScenarioChannels {
            reports: self.tx.clone(),
            mode: rx,
        };
        entry.generation = Some(generation);
        entry.execution = Execution::Live(Box::new(LiveRun::spawn(
            &mut self.workers,
            self.tx.clone(),
            context,
            mode,
            move |cancel| {
                execute(spec, context, budget, channels, async move {
                    let _ = cancel.await;
                })
            },
        )));
        self.transition(id, TaskStatus::Preparing, cause, None)
            .await;
    }

    async fn mode(&mut self, id: TaskId, mode: RunMode) -> Result<(), MonitorCommandError> {
        let entry = self.entry(id)?;
        if entry.can_start() {
            return Ok(());
        }
        entry.desired = mode;
        if let Execution::Live(run) = &entry.execution {
            run.mode.send_if_modified(|current| {
                let changed = *current != mode;
                *current = mode;
                changed
            });
        } else if mode == RunMode::Paused {
            entry.cancel_timer();
            self.transition(
                id,
                TaskStatus::Paused,
                TaskTransitionCause::Command(TaskCommandKind::Pause),
                None,
            )
            .await;
        } else if entry.status() == TaskStatus::Paused {
            entry.attempts = 0;
            self.launch(id, TaskTransitionCause::Command(TaskCommandKind::Resume))
                .await;
        }
        Ok(())
    }

    async fn stop(
        &mut self,
        id: TaskId,
        cause: TaskTransitionCause,
    ) -> Result<(), MonitorCommandError> {
        let entry = self.entry(id)?;
        if !entry.can_start() {
            entry.cancel_timer();
            entry.attempts = 0;
            self.transition(id, TaskStatus::Stopped, cause, None).await;
        }
        Ok(())
    }

    async fn update_from_scenario(&mut self, update: ScenarioUpdate) {
        let id = update.task_id;
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if !entry.is_live(update.generation) {
            return;
        }
        match update.data {
            UpdateData::Built => {
                entry.attempts = 0;
                entry.last_failure = None;
                self.transition(id, TaskStatus::Active, TaskTransitionCause::ScenarioReady, None)
                    .await;
            }
            UpdateData::Progress(progress) if entry.status() == TaskStatus::Preparing => {
                entry.progress = Some(progress.clone());
                self.publish(id, EventData::Preparation(progress)).await;
            }
            UpdateData::Progress(_) => {}
            UpdateData::OperationStarted(operation) => {
                self.publish(id, EventData::OperationStarted(Box::new(operation)))
                    .await
            }
            UpdateData::RunFinished(execution) => self.record(id, execution, true).await,
            UpdateData::Paused => {
                self.transition(
                    id,
                    TaskStatus::Paused,
                    TaskTransitionCause::Command(TaskCommandKind::Pause),
                    None,
                )
                .await
            }
            UpdateData::Resumed => {
                self.transition(
                    id,
                    TaskStatus::Active,
                    TaskTransitionCause::Command(TaskCommandKind::Resume),
                    None,
                )
                .await
            }
        }
    }

    async fn record(&mut self, id: TaskId, execution: ScenarioExecution, counted: bool) {
        let entry = self.entries.get_mut(&id).expect("registered task");
        if counted {
            entry.task.apply_execution(execution.clone());
            entry.task.record_completed_run();
            entry.last_failure = None;
        } else {
            entry.task.store_execution(execution.clone());
        }
        self.publish(
            id,
            EventData::OperationFinished(OperationFinished {
                context: execution.context,
                report: Box::new(execution.report),
            }),
        )
        .await;
    }

    async fn exited(&mut self, context: ScenarioContext, exit: ScenarioExit) {
        let id = context.task_id;
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if !entry.is_live(context.generation) {
            return;
        }
        let Execution::Live(run) = std::mem::take(&mut entry.execution) else {
            unreachable!()
        };
        let closing = run.is_closing();
        match exit {
            ScenarioExit::Completed(last_run) => {
                if let Some(execution) = last_run {
                    self.record(id, execution, true).await;
                }
                let task = &self.entries[&id].task;
                let cause = match task.run_limit() {
                    Some(RunLimit::Runs(limit)) if task.completed_runs() == limit.get() => {
                        TaskTransitionCause::LimitReached { limit: limit.get() }
                    }
                    _ => TaskTransitionCause::ScenarioCompleted,
                };
                self.transition(id, TaskStatus::Completed, cause, None)
                    .await;
            }
            ScenarioExit::PreparationFailed(error) => {
                self.failure(id, ScenarioFailure::Preparation(error), closing)
                    .await
            }
            ScenarioExit::ExecutionFailed(error, execution) => {
                self.record(id, *execution, false).await;
                self.failure(id, ScenarioFailure::Execution(error), closing)
                    .await;
            }
            ScenarioExit::Panicked { stage, reason } => {
                self.failure(id, ScenarioFailure::Panicked { stage, reason }, closing)
                    .await
            }
            ScenarioExit::Cancelled => {}
        }
        for command in run.pending {
            if self.shutdown.is_some()
                && !matches!(command, Command::StopTask { .. } | Command::RemoveTask { .. })
            {
                continue;
            }
            self.command(command).await;
        }
        if self.shutdown.is_some() {
            self.stop_for_shutdown(id).await;
        }
    }

    async fn failure(&mut self, id: TaskId, failure: ScenarioFailure, closing: bool) {
        let entry = self.entries.get_mut(&id).expect("registered task");
        entry.last_failure = Some(failure.clone());
        if closing {
            return;
        }
        let cause = TaskTransitionCause::Failure(Box::new(failure));
        if entry.desired == RunMode::Paused {
            self.transition(id, TaskStatus::Paused, cause, None).await;
            return;
        }
        entry.attempts = entry.attempts.saturating_add(1);
        let delay = self.restart.delay(entry.attempts);
        let deadline = Instant::now() + delay;
        let plan = RestartPlan {
            attempt: entry.attempts,
            scheduled_at: Utc::now() + delay,
        };
        let generation = entry.generation.expect("failed run");
        let tx = self.tx.clone();
        entry.execution = Execution::Backoff(
            self.workers.spawn(
                async move {
                    sleep_until(deadline).await;
                    let _ = tx
                        .send(
                            Command::Respawn {
                                task_id: id,
                                generation,
                            }
                            .into(),
                        )
                        .await;
                }
                .in_current_span(),
            ),
        );
        self.transition(id, TaskStatus::Restarting, cause, Some(plan))
            .await;
    }
}

fn task_id(command: &Command) -> Option<TaskId> {
    match command {
        Command::StartTask { task_id, .. }
        | Command::PauseTask { task_id, .. }
        | Command::ResumeTask { task_id, .. }
        | Command::StopTask { task_id, .. }
        | Command::UpdateTask { task_id, .. }
        | Command::RemoveTask { task_id, .. } => Some(*task_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
