use std::collections::HashMap;

use chrono::Utc;
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinSet,
    time::{Duration, sleep},
};

use crate::monitor::{
    event::{EventData, EventMeta, MonitorEvent},
    runtime::{MonitorSubscription, error::OrchestratorError, restart_policy::RestartPolicy},
    scenario::{Generation, PanicStage, ScenarioCommand, ScenarioReport, run_scenario},
    task::{
        HistoryEntryView, MonitorSnapshot, SpecRevision, Task, TaskConfig, TaskId, TaskStatus,
        TaskView,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskState {
    Idle,
    Starting,
    Active,
    Paused,
    Restarting,
    Completed,
    Stopped,
}

struct TaskEntry {
    task: Task,
    state: TaskState,
    attempts: u32,
    next_generation: Generation,
    generation: Generation,
    scenario: Option<ScenarioHandle>,
}

struct ScenarioHandle {
    generation: Generation,
    abort: tokio::task::AbortHandle,
    cmd: mpsc::Sender<ScenarioCommand>,
}

enum Command {
    AddTask {
        spec: TaskConfig,
        reply: oneshot::Sender<Result<TaskId, OrchestratorError>>,
    },
    RemoveTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    StartTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    PauseTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    ResumeTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    StopTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    UpdateTask {
        task_id: TaskId,
        spec: TaskConfig,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<MonitorSnapshot>,
    },
    SubscribeWithSnapshot {
        reply: oneshot::Sender<MonitorSubscription>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    Respawn {
        task_id: TaskId,
        generation: Generation,
    },
}

pub(super) struct Orchestrator {
    next_id: u64,
    initial_tasks: Vec<TaskId>,
    entries: HashMap<TaskId, TaskEntry>,
    cmd_rx: mpsc::Receiver<Command>,
    cmd_tx: mpsc::Sender<Command>,
    report_rx: mpsc::Receiver<ScenarioReport>,
    report_tx: mpsc::Sender<ScenarioReport>,
    live_tx: broadcast::Sender<MonitorEvent>,
    record_tx: Option<mpsc::Sender<MonitorEvent>>,
    history_sink: Option<mpsc::Sender<MonitorEvent>>,
    scenarios: JoinSet<()>,
    restarts: JoinSet<()>,
    history_writer: JoinSet<()>,
    policy: RestartPolicy,
    seq: u64,
    source: String,
}

impl Orchestrator {
    pub fn new(
        history_sink: Option<mpsc::Sender<MonitorEvent>>,
        restart: RestartPolicy,
        source: String,
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
            record_tx: None,
            history_sink,
            scenarios: JoinSet::new(),
            restarts: JoinSet::new(),
            history_writer: JoinSet::new(),
            policy: restart,
            seq: 0,
            source,
        };
        (
            this,
            OrchestratorHandle {
                cmd_tx,
                live_tx: live_tx.downgrade(),
            },
        )
    }

    pub(super) fn prepare_tasks(&mut self, tasks: Vec<TaskConfig>) -> Vec<TaskId> {
        let ids: Vec<_> = tasks
            .into_iter()
            .map(|spec| self.register_task(spec))
            .collect();
        self.initial_tasks.clone_from(&ids);
        ids
    }

    fn register_task(&mut self, spec: TaskConfig) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        self.entries.insert(
            id,
            TaskEntry {
                task: Task::new(id, spec),
                state: TaskState::Idle,
                attempts: 0,
                next_generation: Generation::first(),
                generation: Generation::first(),
                scenario: None,
            },
        );
        id
    }

    pub async fn run(mut self) {
        if let Some(sink) = self.history_sink.take() {
            let (record_tx, record_rx) = mpsc::channel(1024);
            self.history_writer.spawn(log_writer(record_rx, sink));
            self.record_tx = Some(record_tx);
        }
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
                Some(_) = self.scenarios.join_next() => {},
                Some(_) = self.restarts.join_next() => {},
            }
        }
        self.restarts.shutdown().await;
        self.scenarios.shutdown().await;
        self.record_tx.take();
        while let Some(result) = self.history_writer.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "history writer failed");
            }
        }
    }

    async fn start_initial_tasks(&mut self) {
        let initial_tasks = std::mem::take(&mut self.initial_tasks);
        for &id in &initial_tasks {
            self.emit(id, EventData::TaskAdded).await;
        }
        for id in initial_tasks {
            self.start(id).await.expect("initial task is registered");
        }
    }

    async fn on_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::AddTask { spec, reply } => {
                let id = self.register_task(spec);
                self.emit(id, EventData::TaskAdded).await;
                let _ = reply.send(Ok(id));
                true
            }
            Command::RemoveTask { task_id, reply } => {
                let res = self.remove(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::StartTask { task_id, reply } => {
                let res = self.start(task_id).await;
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
                for entry in self.entries.values_mut() {
                    if let Some(scenario) = entry.scenario.take() {
                        scenario.abort.abort();
                    }
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

    async fn start(&mut self, id: TaskId) -> Result<(), OrchestratorError> {
        let should_start = match self.entries.get(&id) {
            Some(e) => {
                matches!(e.state, TaskState::Idle | TaskState::Stopped | TaskState::Completed)
            }
            None => return Err(OrchestratorError::TaskNotFound(id)),
        };
        if !should_start {
            return Ok(());
        }

        {
            let entry = self.entries.get_mut(&id).expect("checked above");
            entry.task.reset_run();
            entry.attempts = 0;
            entry.state = TaskState::Starting;
        }
        self.spawn_scenario(id);
        self.emit(id, EventData::TaskStarting { attempt: 0 }).await;
        Ok(())
    }

    async fn pause(&mut self, id: TaskId) -> Result<(), OrchestratorError> {
        // forward — шлём команду живому воркеру; set_state — воркера нет, пауза сразу
        let (forward, set_state) = {
            let entry = self
                .entries
                .get_mut(&id)
                .ok_or(OrchestratorError::TaskNotFound(id))?;
            match entry.state {
                TaskState::Starting | TaskState::Active => {
                    (entry.scenario.is_some(), entry.scenario.is_none())
                }
                TaskState::Restarting => (false, true),
                _ => (false, false), // Idle/Stopped/Completed/Paused — no-op
            }
        };

        if forward {
            if let Some(cmd) = self
                .entries
                .get(&id)
                .and_then(|e| e.scenario.as_ref())
                .map(|scenario| scenario.cmd.clone())
            {
                let _ = cmd.send(ScenarioCommand::Pause).await;
            }
        }
        if set_state {
            if let Some(entry) = self.entries.get_mut(&id) {
                entry.state = TaskState::Paused;
            }
            self.emit(id, EventData::TaskPaused).await;
        }
        Ok(())
    }

    async fn resume(&mut self, id: TaskId) -> Result<(), OrchestratorError> {
        // forward — живому воркеру; spawn — воркера нет, поднимаем заново
        let (forward, spawn) = {
            let entry = self
                .entries
                .get_mut(&id)
                .ok_or(OrchestratorError::TaskNotFound(id))?;
            match entry.state {
                TaskState::Paused => (entry.scenario.is_some(), entry.scenario.is_none()),
                _ => (false, false),
            }
        };

        if forward {
            if let Some(cmd) = self
                .entries
                .get(&id)
                .and_then(|e| e.scenario.as_ref())
                .map(|scenario| scenario.cmd.clone())
            {
                let _ = cmd.send(ScenarioCommand::Resume).await;
            }
        }
        if spawn {
            {
                let entry = self.entries.get_mut(&id).expect("checked above");
                entry.attempts = 0;
                entry.state = TaskState::Starting;
            }
            self.spawn_scenario(id);
            self.emit(id, EventData::TaskStarting { attempt: 0 }).await;
        }
        Ok(())
    }

    async fn stop(&mut self, id: TaskId) -> Result<(), OrchestratorError> {
        let stopped = {
            let entry = self
                .entries
                .get_mut(&id)
                .ok_or(OrchestratorError::TaskNotFound(id))?;
            match entry.state {
                TaskState::Idle | TaskState::Stopped => false, // нечего останавливать
                _ => {
                    if let Some(scenario) = entry.scenario.take() {
                        scenario.abort.abort();
                    }
                    entry.attempts = 0;
                    entry.state = TaskState::Stopped;
                    true
                }
            }
        };
        if stopped {
            self.emit(id, EventData::TaskStopped).await;
        }
        Ok(())
    }

    async fn update(&mut self, id: TaskId, spec: TaskConfig) -> Result<(), OrchestratorError> {
        let respawn = {
            let entry = self
                .entries
                .get_mut(&id)
                .ok_or(OrchestratorError::TaskNotFound(id))?;
            entry.task.update_config(spec);
            entry.attempts = 0;
            matches!(entry.state, TaskState::Starting | TaskState::Active | TaskState::Restarting)
        };
        let paused = matches!(self.entries.get(&id), Some(e) if e.state == TaskState::Paused);

        self.emit(id, EventData::TaskSpecUpdated).await;

        if respawn {
            {
                let entry = self.entries.get_mut(&id).expect("checked above");
                if let Some(scenario) = entry.scenario.take() {
                    scenario.abort.abort();
                }
                entry.state = TaskState::Starting;
            }
            self.spawn_scenario(id);
            self.emit(id, EventData::TaskStarting { attempt: 0 }).await;
        } else if paused {
            if let Some(entry) = self.entries.get_mut(&id) {
                if let Some(scenario) = entry.scenario.take() {
                    scenario.abort.abort();
                }
            }
        }
        Ok(())
    }

    async fn remove(&mut self, id: TaskId) -> Result<(), OrchestratorError> {
        let entry = self
            .entries
            .remove(&id)
            .ok_or(OrchestratorError::TaskNotFound(id))?;
        if let Some(scenario) = &entry.scenario {
            scenario.abort.abort();
        }
        let generation = entry.generation;
        let revision = entry.task.revision();
        let metadata = self.next_meta(id, generation, revision);
        self.emit_event(MonitorEvent {
            metadata,
            data: EventData::TaskRemoved,
        })
        .await;
        Ok(())
    }

    async fn on_report(&mut self, rep: ScenarioReport) {
        let (task_id, generation) = rep.key();
        let mut completed = false;

        let (data, respawn_after): (Option<EventData>, Option<Duration>) = {
            let Some(entry) = self.entries.get_mut(&task_id) else {
                return;
            };
            if entry.scenario.as_ref().map(|scenario| scenario.generation) != Some(generation) {
                return;
            }

            match rep {
                ScenarioReport::Built { .. } => {
                    entry.attempts = 0;
                    entry.task.clear_error();
                    entry.state = TaskState::Active;
                    (Some(EventData::TaskStarted), None)
                }
                ScenarioReport::BuildFailed { error, .. } => {
                    entry.scenario = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(error.to_string());
                    (
                        Some(EventData::BuildFailed {
                            error,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                ScenarioReport::BuildPanicked { reason, .. } => {
                    entry.scenario = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(format!("build: {reason}"));
                    (
                        Some(EventData::ScenarioPanicked {
                            stage: PanicStage::Build,
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                ScenarioReport::Executed { execution, .. } => {
                    entry.task.apply_execution(execution.clone());
                    if limit_reached(entry) {
                        if let Some(scenario) = entry.scenario.take() {
                            scenario.abort.abort();
                        }
                        entry.state = TaskState::Completed;
                        completed = true;
                    }
                    (
                        Some(EventData::ExecutionFinished {
                            execution,
                            metrics: entry.task.metrics(),
                        }),
                        None,
                    )
                }
                ScenarioReport::ExecutionFailed { error, .. } => {
                    entry.scenario = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(error.to_string());
                    (
                        Some(EventData::ExecutionFailed {
                            error,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                ScenarioReport::ExecutionPanicked { reason, .. } => {
                    entry.scenario = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(format!("execution: {reason}"));
                    (
                        Some(EventData::ScenarioPanicked {
                            stage: PanicStage::Execution,
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                ScenarioReport::Panicked { reason, .. } => {
                    entry.scenario = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(format!("scenario: {reason}"));
                    (
                        Some(EventData::ScenarioPanicked {
                            stage: PanicStage::Runtime,
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                ScenarioReport::Paused { .. } => {
                    entry.state = TaskState::Paused;
                    (Some(EventData::TaskPaused), None)
                }
                ScenarioReport::Resumed { .. } => {
                    entry.state = TaskState::Active;
                    (Some(EventData::TaskResumed), None)
                }
            }
        };

        if let Some(data) = data {
            self.emit(task_id, data).await;
        }
        if completed {
            self.emit(task_id, EventData::TaskCompleted).await;
        }
        if let Some(delay) = respawn_after {
            self.schedule_respawn(task_id, generation, delay);
        }
    }

    fn spawn_scenario(&mut self, task_id: TaskId) {
        let Some(entry) = self.entries.get_mut(&task_id) else {
            return;
        };
        let generation = entry.next_generation;
        entry.next_generation = generation.next();
        entry.generation = generation;
        let scenario = entry.task.config().scenario().clone();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let report_tx = self.report_tx.clone();
        let abort = self
            .scenarios
            .spawn(run_scenario(scenario, task_id, generation, report_tx, cmd_rx));
        entry.scenario = Some(ScenarioHandle {
            generation,
            abort,
            cmd: cmd_tx,
        });
    }

    fn schedule_respawn(&mut self, task_id: TaskId, generation: Generation, delay: Duration) {
        let cmd_tx = self.cmd_tx.clone();
        self.restarts.spawn(async move {
            sleep(delay).await;
            let _ = cmd_tx
                .send(Command::Respawn {
                    task_id,
                    generation,
                })
                .await;
        });
    }

    async fn maybe_respawn(&mut self, task_id: TaskId, generation: Generation) {
        let should = matches!(
            self.entries.get(&task_id),
            Some(e) if e.state == TaskState::Restarting
                && e.scenario.is_none()
                && e.generation == generation
        );
        if should {
            let attempt = self.entries.get(&task_id).map_or(0, |e| e.attempts);
            {
                let entry = self.entries.get_mut(&task_id).expect("checked above");
                entry.state = TaskState::Starting;
            }
            self.spawn_scenario(task_id);
            self.emit(task_id, EventData::TaskStarting { attempt })
                .await;
        }
    }

    async fn emit(&mut self, task_id: TaskId, data: EventData) {
        let Some((generation, revision)) = self
            .entries
            .get(&task_id)
            .map(|e| (e.generation, e.task.revision()))
        else {
            return;
        };
        let metadata = self.next_meta(task_id, generation, revision);
        self.emit_event(MonitorEvent { metadata, data }).await;
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
            source: self.source.clone(),
            task_id,
            generation,
            revision,
        }
    }

    async fn emit_event(&mut self, ev: MonitorEvent) {
        if self.live_tx.receiver_count() > 0 {
            let _ = self.live_tx.send(ev.clone());
        }
        if let Some(record_tx) = &self.record_tx {
            let _ = record_tx.send(ev).await;
        }
    }

    fn view(&self, entry: &TaskEntry) -> TaskView {
        let t = &entry.task;
        let config = t.config();
        let history = t
            .history()
            .iter()
            .map(|h| HistoryEntryView {
                timestamp: h.timestamp,
                result: h.result.clone(),
            })
            .collect();

        TaskView {
            id: t.id(),
            name: config.name().to_string(),
            revision: t.revision(),
            created_at: *t.created_at(),
            config_updated_at: *t.config_updated_at(),
            updated_at: *t.updated_at(),
            target: config.scenario().target(),
            status: status(entry),
            deep_history: config.deep_history(),
            scenario: config.scenario().clone(),
            limit: config.limit(),
            metrics: t.metrics(),
            result: t.last_result().cloned(),
            history,
            health: t.health().clone(),
        }
    }

    pub(super) fn snapshot(&self) -> MonitorSnapshot {
        let mut tasks: Vec<TaskView> = self.entries.values().map(|e| self.view(e)).collect();
        tasks.sort_by_key(|v| v.id);
        MonitorSnapshot {
            source: self.source.clone(),
            next_seq: self.seq,
            tasks,
        }
    }
}

fn status(entry: &TaskEntry) -> TaskStatus {
    match entry.state {
        TaskState::Idle => TaskStatus::Idle,
        TaskState::Starting => TaskStatus::Starting,
        TaskState::Active => TaskStatus::Active,
        TaskState::Paused => TaskStatus::Paused,
        TaskState::Restarting => TaskStatus::Restarting,
        TaskState::Completed => TaskStatus::Completed,
        TaskState::Stopped => TaskStatus::Stopped,
    }
}

fn limit_reached(entry: &TaskEntry) -> bool {
    let limit = entry.task.config().limit();
    limit > 0 && entry.task.metrics().executions >= limit
}

async fn log_writer(mut rx: mpsc::Receiver<MonitorEvent>, hist_tx: mpsc::Sender<MonitorEvent>) {
    while let Some(ev) = rx.recv().await {
        if hist_tx.send(ev).await.is_err() {
            tracing::warn!("history sink closed, log writer stops");
            break;
        }
    }
}

#[derive(Clone)]
pub(super) struct OrchestratorHandle {
    cmd_tx: mpsc::Sender<Command>,
    live_tx: broadcast::WeakSender<MonitorEvent>,
}

impl OrchestratorHandle {
    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddTask { spec, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::RemoveTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StartTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::PauseTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn resume_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::ResumeTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StopTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::UpdateTask {
                task_id,
                spec,
                reply: tx,
            })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::GetSnapshot { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MonitorEvent> {
        self.live_tx.upgrade().map_or_else(
            || {
                let (_, events) = broadcast::channel(1);
                events
            },
            |sender| sender.subscribe(),
        )
    }

    pub async fn subscribe_with_snapshot(&self) -> Result<MonitorSubscription, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubscribeWithSnapshot { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Shutdown { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;
    use crate::{
        monitor::{
            probe::{SnmpGetSpec, SnmpSetSpec},
            scenario::{
                PollSchedule, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError,
                ScenarioOutcome, ScenarioSpec, SnmpGetScenarioSpec, SnmpSetScenarioSpec,
            },
            task::ExecutionMetrics,
        },
        polling::{AttemptConfig, FatalError, Response},
    };

    fn active_task(limit: u64) -> (Orchestrator, broadcast::Receiver<MonitorEvent>, TaskId) {
        let (mut orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), "test".to_string());
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
        let task = Task::new(
            task_id,
            TaskConfig::try_new("test".to_string(), scenario, limit, 3).unwrap(),
        );
        let (cmd, _cmd_rx) = mpsc::channel(8);
        let join = tokio::spawn(std::future::pending::<()>());
        orchestrator.entries.insert(
            task_id,
            TaskEntry {
                task,
                state: TaskState::Active,
                attempts: 0,
                next_generation: Generation::first().next(),
                generation: Generation::first(),
                scenario: Some(ScenarioHandle {
                    generation: Generation::first(),
                    abort: join.abort_handle(),
                    cmd,
                }),
            },
        );
        (orchestrator, events, task_id)
    }

    fn execution(at: i64, success: bool) -> ScenarioExecution {
        let finished_at = DateTime::from_timestamp(at, 0).unwrap();
        let response = if success {
            Response::Success {
                timestamp: finished_at,
                attempts: 3,
                errors: vec![],
                elapsed: Duration::from_millis(25),
                payload: vec![],
            }
        } else {
            Response::NoResponse {
                timestamp: finished_at,
                attempts: 3,
                errors: vec![],
                elapsed: Duration::from_millis(30),
            }
        };
        ScenarioExecution {
            finished_at,
            elapsed: Duration::from_millis(if success { 50 } else { 80 }),
            outcome: ScenarioOutcome::SnmpGet(response),
        }
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
        TaskConfig::try_new(name.to_string(), scenario, 0, 3).unwrap()
    }

    fn control_scenario(orchestrator: &mut Orchestrator, task_id: TaskId) {
        let entry = orchestrator.entries.get_mut(&task_id).unwrap();
        if let Some(scenario) = entry.scenario.take() {
            scenario.abort.abort();
        }
        let (cmd, _commands) = mpsc::channel(8);
        let join = tokio::spawn(std::future::pending::<()>());
        entry.scenario = Some(ScenarioHandle {
            generation: entry.generation,
            abort: join.abort_handle(),
            cmd,
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
        orchestrator
            .on_report(ScenarioReport::Executed {
                task_id,
                generation: Generation::first(),
                execution,
            })
            .await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_scenarios_and_restart_tasks() {
        let (mut orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), "shutdown-test".to_string());
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
            Orchestrator::new(None, RestartPolicy::default(), "abort-test".to_string());
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
        let (orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), "subscription-test".to_string());
        let runtime = tokio::spawn(orchestrator.run());
        let first = handle.add_task(set_config("first")).await.unwrap();
        let second = handle.add_task(set_config("second")).await.unwrap();

        let MonitorSubscription {
            snapshot,
            mut events,
        } = handle.subscribe_with_snapshot().await.unwrap();
        assert_eq!(snapshot.source, "subscription-test");
        assert_eq!(snapshot.next_seq, 2);
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, first);
        assert_eq!(snapshot.tasks[1].id, second);
        assert!(matches!(events.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        let third = handle.add_task(set_config("third")).await.unwrap();
        let event = events.recv().await.unwrap();
        assert_eq!(event.metadata.source, snapshot.source);
        assert_eq!(event.metadata.seq, snapshot.next_seq);
        assert_eq!(event.metadata.task_id, third);
        assert!(matches!(event.data, EventData::TaskAdded));
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
        assert_eq!(current.source, snapshot.source);
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
            Orchestrator::new(None, RestartPolicy::default(), "subscription-test".to_string());
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
            assert!(matches!(event.data, EventData::TaskAdded));
        }
        handle.shutdown().await.unwrap();
        runtime.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_closes_when_the_orchestrator_stops_with_handles_alive() {
        let (orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), "subscription-test".to_string());
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
            Err(OrchestratorError::ChannelClosed)
        ));
    }

    #[tokio::test]
    async fn subscription_snapshot_recovers_state_after_lagging() {
        let (orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), "subscription-test".to_string());
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
        assert_eq!(snapshot.source, subscription.snapshot.source);
        assert_eq!(snapshot.next_seq, 1026);
        assert_eq!(snapshot.tasks[0].name, "updated");
        assert_eq!(snapshot.tasks[0].revision, SpecRevision::new(1026));
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
    async fn events_share_source_and_sequence_across_tasks() {
        let source = "contract-test-monitor".to_string();
        let (mut orchestrator, handle) =
            Orchestrator::new(None, RestartPolicy::default(), source.clone());
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
            assert_eq!(event.metadata.source, source);
            assert_eq!(event.metadata.seq, u64::try_from(seq).unwrap());
            assert_eq!(event.metadata.task_id, task_id);
            assert_eq!(event.metadata.revision, revision);
            assert_eq!(event.metadata.generation, Generation::first());
            match seq {
                0 | 1 => assert!(matches!(event.data, EventData::TaskAdded)),
                2 => assert!(matches!(event.data, EventData::TaskSpecUpdated)),
                3 => assert!(matches!(event.data, EventData::TaskRemoved)),
                _ => unreachable!(),
            }
        }
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn no_response_is_an_execution_and_does_not_restart_the_scenario() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let first = execution(100, false);
        execute(&mut orchestrator, task_id, first.clone()).await;

        let event = events.try_recv().unwrap();
        let EventData::ExecutionFinished { execution, metrics } = event.data else {
            panic!("expected an execution event");
        };
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.status, TaskStatus::Active);
        assert_eq!(task.metrics.executions, 1);
        assert_eq!(task.metrics.failed, 1);
        assert_eq!(task.metrics.current_duration, first.elapsed);
        assert_eq!(task.metrics, metrics);
        assert_eq!(task.health.consecutive_failures, 1);
        assert_eq!(task.health.last_execution_at, Some(first.finished_at));
        assert_eq!(task.result.as_ref().unwrap().finished_at, execution.finished_at);
        assert!(orchestrator.entries[&task_id].scenario.is_some());

        execute(&mut orchestrator, task_id, self::execution(200, true)).await;
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.metrics.executions, 2);
        assert_eq!(task.metrics.successful, 1);
        assert_eq!(task.metrics.failed, 1);
        assert_eq!(task.health.consecutive_failures, 0);
        assert_eq!(task.history.len(), 1);
        assert_eq!(task.history[0].timestamp, first.finished_at);
        assert_eq!(task.history[0].result.finished_at, first.finished_at);
    }

    #[tokio::test]
    async fn final_execution_is_published_before_task_completion() {
        let (mut orchestrator, mut events, task_id) = active_task(1);
        execute(&mut orchestrator, task_id, execution(100, true)).await;

        let executed = events.try_recv().unwrap();
        let completed = events.try_recv().unwrap();
        assert!(matches!(executed.data, EventData::ExecutionFinished { .. }));
        assert!(matches!(completed.data, EventData::TaskCompleted));
        assert_eq!(completed.metadata.seq, executed.metadata.seq + 1);
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Completed);
        assert_eq!(snapshot.tasks[0].limit, 1);
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
        assert!(matches!(events.try_recv().unwrap().data, EventData::ExecutionFinished { .. }));
        assert!(events.try_recv().is_err());

        execute(&mut orchestrator, task_id, execution(200, false)).await;

        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics.executions, 2);
        assert_eq!(snapshot.tasks[0].metrics.failed, 2);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Completed);
        assert!(matches!(events.try_recv().unwrap().data, EventData::ExecutionFinished { .. }));
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskCompleted));
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn execution_from_another_generation_is_ignored() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(ScenarioReport::Executed {
                task_id,
                generation: Generation::first().next(),
                execution: execution(100, true),
            })
            .await;

        assert!(events.try_recv().is_err());
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].metrics, ExecutionMetrics::default());
        assert!(snapshot.tasks[0].result.is_none());
    }

    #[tokio::test]
    async fn execution_error_preserves_its_type_and_restarts_the_scenario() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(ScenarioReport::ExecutionFailed {
                task_id,
                generation: Generation::first(),
                error: ScenarioExecutionError::Polling(FatalError {
                    message: "invalid request".to_string(),
                }),
            })
            .await;

        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::ExecutionFailed {
                error: ScenarioExecutionError::Polling(FatalError { message }),
                retry_in: Some(_),
            } if message == "invalid request"
        ));
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
        assert_eq!(snapshot.tasks[0].metrics.executions, 0);
        assert!(snapshot.tasks[0].result.is_none());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
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
            let EventData::BuildFailed {
                error: reported,
                retry_in,
            } = event.data
            else {
                panic!("expected a build failure event");
            };
            assert_eq!(reported, error);
            assert_eq!(retry_in, Some(Duration::from_secs(1)));
            let snapshot = orchestrator.snapshot();
            assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
            assert_eq!(snapshot.tasks[0].health.last_error, Some(error.to_string()));
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
            assert!(matches!(
                event.data,
                EventData::ScenarioPanicked { stage: actual, reason, retry_in: Some(delay) }
                    if actual == stage && reason == "panic" && delay == Duration::from_secs(1)
            ));
            let snapshot = orchestrator.snapshot();
            assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
            assert_eq!(snapshot.tasks[0].metrics.executions, 0);
            assert_eq!(
                snapshot.tasks[0].health.last_error.as_deref(),
                Some(format!("{label}: panic").as_str())
            );
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
        orchestrator.start(task_id).await.unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(matches!(events.try_recv().unwrap().data, EventData::ExecutionFinished { .. }));
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStopped));
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskSpecUpdated));
        let starting = events.try_recv().unwrap();
        assert!(matches!(starting.data, EventData::TaskStarting { attempt: 0 }));
        let snapshot = orchestrator.snapshot();
        let task = &snapshot.tasks[0];
        assert_eq!(task.name, "fresh");
        assert_eq!(task.limit, 0);
        assert_eq!(task.target, "127.0.0.2:1161");
        assert_eq!(task.status, TaskStatus::Starting);
        assert_eq!(task.metrics, ExecutionMetrics::default());
        assert!(
            matches!(&task.scenario, ScenarioSpec::SnmpSet(spec) if spec.schedule().duration() == Duration::from_secs(2))
        );
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: starting.metadata.generation,
            })
            .await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStarted));
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
    }

    #[tokio::test]
    async fn pause_and_resume_wait_for_acknowledgements_and_stop_rejects_late_reports() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let generation = orchestrator.entries[&task_id].generation;
        let (cmd, mut commands) = mpsc::channel(8);
        let scenario = orchestrator
            .entries
            .get_mut(&task_id)
            .unwrap()
            .scenario
            .as_mut()
            .unwrap();
        scenario.cmd = cmd;
        let abort = scenario.abort.clone();

        orchestrator.pause(task_id).await.unwrap();
        assert!(matches!(commands.recv().await, Some(ScenarioCommand::Pause)));
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation,
            })
            .await;
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskPaused));

        orchestrator.pause(task_id).await.unwrap();
        assert!(commands.try_recv().is_err());
        assert!(events.try_recv().is_err());
        orchestrator.resume(task_id).await.unwrap();
        assert!(matches!(commands.recv().await, Some(ScenarioCommand::Resume)));
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert!(events.try_recv().is_err());
        orchestrator
            .on_report(ScenarioReport::Resumed {
                task_id,
                generation,
            })
            .await;
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Active);
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskResumed));

        orchestrator.resume(task_id).await.unwrap();
        assert!(commands.try_recv().is_err());
        assert!(events.try_recv().is_err());
        orchestrator.stop(task_id).await.unwrap();
        wait_for_abort(abort).await;
        assert!(commands.recv().await.is_none());
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStopped));
        orchestrator
            .on_report(ScenarioReport::Executed {
                task_id,
                generation,
                execution: execution(100, true),
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
    async fn active_spec_update_replaces_scenario_and_preserves_execution_metrics() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        execute(&mut orchestrator, task_id, execution(100, true)).await;
        events.try_recv().unwrap();
        let metrics = orchestrator.snapshot().tasks[0].metrics;
        let previous_generation = orchestrator.entries[&task_id].generation;
        let abort = orchestrator.entries[&task_id]
            .scenario
            .as_ref()
            .unwrap()
            .abort
            .clone();

        orchestrator
            .update(task_id, set_config("updated"))
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        wait_for_abort(abort).await;
        let updated = events.try_recv().unwrap();
        let starting = events.try_recv().unwrap();
        assert!(matches!(updated.data, EventData::TaskSpecUpdated));
        assert!(matches!(starting.data, EventData::TaskStarting { attempt: 0 }));
        assert_eq!(updated.metadata.task_id, task_id);
        assert_eq!(updated.metadata.revision, SpecRevision::new(2));
        assert_eq!(updated.metadata.generation, previous_generation);
        assert_eq!(starting.metadata.source, updated.metadata.source);
        assert_eq!(starting.metadata.task_id, updated.metadata.task_id);
        assert_eq!(starting.metadata.seq, updated.metadata.seq + 1);
        assert_eq!(starting.metadata.revision, SpecRevision::new(2));
        assert_eq!(starting.metadata.generation, previous_generation.next());

        for report in [
            ScenarioReport::Built {
                task_id,
                generation: previous_generation,
            },
            ScenarioReport::BuildFailed {
                task_id,
                generation: previous_generation,
                error: ScenarioBuildError::Other("obsolete".to_string()),
            },
            ScenarioReport::Executed {
                task_id,
                generation: previous_generation,
                execution: execution(200, false),
            },
            ScenarioReport::Paused {
                task_id,
                generation: previous_generation,
            },
        ] {
            orchestrator.on_report(report).await;
        }
        assert!(events.try_recv().is_err());
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Starting);
        assert_eq!(snapshot.tasks[0].name, "updated");
        assert_eq!(snapshot.tasks[0].metrics, metrics);
        assert_eq!(
            snapshot.tasks[0]
                .result
                .as_ref()
                .unwrap()
                .finished_at
                .timestamp(),
            100
        );
        assert!(
            matches!(&snapshot.tasks[0].scenario, ScenarioSpec::SnmpSet(spec) if spec.schedule().duration() == Duration::from_secs(2))
        );
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: starting.metadata.generation,
            })
            .await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStarted));
        assert_eq!(orchestrator.snapshot().tasks[0].metrics, metrics);
    }

    #[tokio::test]
    async fn paused_spec_update_defers_new_scenario_until_resume() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let previous_generation = orchestrator.entries[&task_id].generation;
        let abort = orchestrator.entries[&task_id]
            .scenario
            .as_ref()
            .unwrap()
            .abort
            .clone();
        orchestrator
            .on_report(ScenarioReport::Paused {
                task_id,
                generation: previous_generation,
            })
            .await;
        events.try_recv().unwrap();
        orchestrator
            .update(task_id, set_config("updated while paused"))
            .await
            .unwrap();
        wait_for_abort(abort).await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskSpecUpdated));
        assert!(events.try_recv().is_err());
        assert!(orchestrator.report_rx.try_recv().is_err());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);
        assert_eq!(orchestrator.entries[&task_id].generation, previous_generation);
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: previous_generation,
            })
            .await;
        assert!(events.try_recv().is_err());
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);

        orchestrator.resume(task_id).await.unwrap();
        control_scenario(&mut orchestrator, task_id);
        let starting = events.try_recv().unwrap();
        assert!(matches!(starting.data, EventData::TaskStarting { attempt: 0 }));
        assert_eq!(starting.metadata.revision, SpecRevision::new(2));
        assert_eq!(starting.metadata.generation, previous_generation.next());
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].name, "updated while paused");
        assert!(
            matches!(&snapshot.tasks[0].scenario, ScenarioSpec::SnmpSet(spec) if spec.schedule().duration() == Duration::from_secs(2))
        );
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: starting.metadata.generation,
            })
            .await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStarted));
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

        orchestrator
            .update(task_id, set_config("latest"))
            .await
            .unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskSpecUpdated));
        let starting = events.try_recv().unwrap();
        assert!(matches!(starting.data, EventData::TaskStarting { attempt: 0 }));
        let generation = starting.metadata.generation;
        assert_eq!(generation, previous_generation.next());
        fail_scenario(&mut orchestrator, task_id).await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::BuildFailed { .. }));
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
        assert!(matches!(restarting.data, EventData::TaskStarting { attempt: 1 }));
        assert_eq!(restarting.metadata.generation, generation.next());
        assert_eq!(restarting.metadata.revision, SpecRevision::new(2));
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].name, "latest");
        assert!(
            matches!(&snapshot.tasks[0].scenario, ScenarioSpec::SnmpSet(spec) if spec.schedule().duration() == Duration::from_secs(2))
        );
        orchestrator
            .on_report(ScenarioReport::Built {
                task_id,
                generation: restarting.metadata.generation,
            })
            .await;
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStarted));
        assert_eq!(orchestrator.entries[&task_id].attempts, 0);
    }

    #[tokio::test]
    async fn pausing_backoff_blocks_respawn_and_resume_resets_retry_attempts() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator.entries.get_mut(&task_id).unwrap().task =
            Task::new(task_id, set_config("resumed"));
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
        assert!(matches!(events.try_recv().unwrap().data, EventData::TaskPaused));
        orchestrator.maybe_respawn(task_id, generation).await;
        assert!(events.try_recv().is_err());
        assert!(orchestrator.entries[&task_id].scenario.is_none());
        assert_eq!(orchestrator.snapshot().tasks[0].status, TaskStatus::Paused);

        orchestrator.resume(task_id).await.unwrap();
        control_scenario(&mut orchestrator, task_id);
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::TaskStarting { attempt: 0 }
        ));
        assert_eq!(orchestrator.entries[&task_id].attempts, 0);
        fail_scenario(&mut orchestrator, task_id).await;
        assert_eq!(orchestrator.entries[&task_id].attempts, 1);
        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::BuildFailed { retry_in: Some(delay), .. } if delay == Duration::from_secs(1)
        ));
    }
}
