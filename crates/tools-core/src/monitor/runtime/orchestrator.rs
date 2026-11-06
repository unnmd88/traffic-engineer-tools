use std::collections::HashMap;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::monitor::{
    event::{EventData, EventMeta, MonitorEvent},
    runtime::{
        error::OrchestratorError,
        restart_policy::RestartPolicy,
        worker::{Generation, WorkerCmd, WorkerReport},
    },
    scenario::Scenario,
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
    worker: Option<WorkerHandle>,
}

struct WorkerHandle {
    generation: Generation,
    abort: tokio::task::AbortHandle,
    cmd: mpsc::Sender<WorkerCmd>,
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
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    Respawn {
        task_id: TaskId,
    },
}

pub struct Orchestrator {
    next_id: u64,
    entries: HashMap<TaskId, TaskEntry>,
    cmd_rx: mpsc::Receiver<Command>,
    cmd_tx: mpsc::Sender<Command>,
    report_rx: mpsc::Receiver<WorkerReport>,
    report_tx: mpsc::Sender<WorkerReport>,
    live_tx: broadcast::Sender<MonitorEvent>,
    record_tx: Option<mpsc::Sender<MonitorEvent>>,
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

        let record_tx = history_sink.as_ref().map(|sink| {
            let (record_tx, record_rx) = mpsc::channel(1024);
            tokio::spawn(log_writer(record_rx, Some(sink.clone())));
            record_tx
        });

        let this = Self {
            next_id: 1,
            entries: HashMap::new(),
            cmd_rx,
            cmd_tx: cmd_tx.clone(),
            report_rx,
            report_tx,
            live_tx: live_tx.clone(),
            record_tx,
            policy: restart,
            seq: 0,
            source,
        };
        (this, OrchestratorHandle { cmd_tx, live_tx })
    }

    pub async fn run(mut self) {
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
            }
        }
    }

    async fn on_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::AddTask { spec, reply } => {
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
                        worker: None,
                    },
                );
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
            Command::Shutdown { reply } => {
                for entry in self.entries.values_mut() {
                    if let Some(w) = entry.worker.take() {
                        w.abort.abort();
                    }
                }
                self.entries.clear();
                let _ = reply.send(());
                false
            }
            Command::Respawn { task_id } => {
                self.maybe_respawn(task_id).await;
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
        self.spawn_worker(id);
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
                    (entry.worker.is_some(), entry.worker.is_none())
                }
                TaskState::Restarting => (false, true),
                _ => (false, false), // Idle/Stopped/Completed/Paused — no-op
            }
        };

        if forward {
            if let Some(cmd) = self
                .entries
                .get(&id)
                .and_then(|e| e.worker.as_ref())
                .map(|w| w.cmd.clone())
            {
                let _ = cmd.send(WorkerCmd::Pause).await;
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
                TaskState::Paused => (entry.worker.is_some(), entry.worker.is_none()),
                _ => (false, false),
            }
        };

        if forward {
            if let Some(cmd) = self
                .entries
                .get(&id)
                .and_then(|e| e.worker.as_ref())
                .map(|w| w.cmd.clone())
            {
                let _ = cmd.send(WorkerCmd::Resume).await;
            }
        }
        if spawn {
            {
                let entry = self.entries.get_mut(&id).expect("checked above");
                entry.state = TaskState::Starting;
            }
            self.spawn_worker(id);
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
                    if let Some(w) = entry.worker.take() {
                        w.abort.abort();
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
            entry.task.update_spec(spec);
            matches!(entry.state, TaskState::Starting | TaskState::Active | TaskState::Restarting)
        };
        let paused = matches!(self.entries.get(&id), Some(e) if e.state == TaskState::Paused);

        self.emit(id, EventData::TaskSpecUpdated).await;

        if respawn {
            {
                let entry = self.entries.get_mut(&id).expect("checked above");
                if let Some(w) = entry.worker.take() {
                    w.abort.abort();
                }
                entry.state = TaskState::Starting;
            }
            self.spawn_worker(id);
        } else if paused {
            // спека сменилась — старый адаптер недействителен; убиваем воркера,
            // остаёмся Paused, Resume поднимет уже с новой спекой.
            if let Some(entry) = self.entries.get_mut(&id) {
                if let Some(w) = entry.worker.take() {
                    w.abort.abort();
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
        if let Some(w) = &entry.worker {
            w.abort.abort();
        }
        let generation = entry.generation;
        let revision = entry.task.spec().revision();
        let metadata = self.next_meta(id, generation, revision);
        self.emit_event(MonitorEvent {
            metadata,
            data: EventData::TaskRemoved,
        })
        .await;
        Ok(())
    }

    async fn on_report(&mut self, rep: WorkerReport) {
        let (task_id, generation) = rep.key();
        let mut completed = false;

        let (data, respawn_after): (Option<EventData>, Option<Duration>) = {
            let Some(entry) = self.entries.get_mut(&task_id) else {
                return;
            };
            if entry.worker.as_ref().map(|w| w.generation) != Some(generation) {
                return;
            }

            match rep {
                WorkerReport::Built { .. } => {
                    entry.attempts = 0;
                    entry.task.clear_error();
                    entry.state = TaskState::Active;
                    (Some(EventData::TaskStarted), None)
                }
                WorkerReport::BuildFailed { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(reason.clone());
                    (
                        Some(EventData::BuildFailed {
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::BuildTimeout { .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error("build timeout".to_string());
                    (
                        Some(EventData::BuildFailed {
                            reason: "build timeout".to_string(),
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::BuildPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    let reason = format!("build: {reason}");
                    entry.task.record_error(reason.clone());
                    (
                        Some(EventData::WorkerPanicked {
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::Executed { execution, .. } => {
                    entry.task.apply_execution(execution.clone());
                    if limit_reached(entry) {
                        if let Some(w) = entry.worker.take() {
                            w.abort.abort();
                        }
                        entry.state = TaskState::Completed;
                        completed = true;
                    }
                    (
                        Some(EventData::ProbeExecuted {
                            execution,
                            metrics: entry.task.metrics(),
                        }),
                        None,
                    )
                }
                WorkerReport::ExecutionFailed { error, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    entry.task.record_error(error.to_string());
                    (
                        Some(EventData::ProbeError {
                            error,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::ExecutionPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    let reason = format!("execution: {reason}");
                    entry.task.record_error(reason.clone());
                    (
                        Some(EventData::WorkerPanicked {
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::WorkerPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    let reason = format!("worker: {reason}");
                    entry.task.record_error(reason.clone());
                    (
                        Some(EventData::WorkerPanicked {
                            reason,
                            retry_in: Some(delay),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::Paused { .. } => {
                    entry.state = TaskState::Paused;
                    (Some(EventData::TaskPaused), None)
                }
                WorkerReport::Resumed { .. } => {
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
            self.schedule_respawn(task_id, delay);
        }
    }

    fn spawn_worker(&mut self, task_id: TaskId) {
        let Some(entry) = self.entries.get_mut(&task_id) else {
            return;
        };
        let generation = entry.next_generation;
        entry.next_generation = generation.next();
        entry.generation = generation;
        let probe_spec = entry.task.spec().probe().clone();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let report_tx = self.report_tx.clone();
        let join = tokio::spawn(async move {
            match Scenario::try_from(probe_spec) {
                Ok(scenario) => scenario.run(task_id, generation, report_tx, cmd_rx).await,
                Err(error) => {
                    let _ = report_tx
                        .send(WorkerReport::BuildFailed {
                            task_id,
                            generation,
                            reason: error.to_string(),
                        })
                        .await;
                }
            }
        });
        entry.worker = Some(WorkerHandle {
            generation,
            abort: join.abort_handle(),
            cmd: cmd_tx,
        });
    }

    fn schedule_respawn(&self, task_id: TaskId, delay: Duration) {
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            sleep(delay).await;
            let _ = cmd_tx.send(Command::Respawn { task_id }).await;
        });
    }

    async fn maybe_respawn(&mut self, task_id: TaskId) {
        let should = matches!(
            self.entries.get(&task_id),
            Some(e) if e.state == TaskState::Restarting && e.worker.is_none()
        );
        if should {
            let attempt = self.entries.get(&task_id).map_or(0, |e| e.attempts);
            {
                let entry = self.entries.get_mut(&task_id).expect("checked above");
                entry.state = TaskState::Starting;
            }
            self.spawn_worker(task_id);
            self.emit(task_id, EventData::TaskStarting { attempt })
                .await;
        }
    }

    async fn emit(&mut self, task_id: TaskId, data: EventData) {
        let Some((generation, revision)) = self
            .entries
            .get(&task_id)
            .map(|e| (e.generation, e.task.spec().revision()))
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
        let spec = t.spec();
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
            name: spec.name().to_string(),
            revision: spec.revision(),
            target: spec.probe().target(),
            status: status(entry),
            deep_history: spec.deep_history(),
            spec: spec.probe().clone(),
            metrics: t.metrics(),
            result: t.last_result().cloned(),
            history,
            health: t.health().clone(),
        }
    }

    fn snapshot(&self) -> MonitorSnapshot {
        let mut tasks: Vec<TaskView> = self.entries.values().map(|e| self.view(e)).collect();
        tasks.sort_by_key(|v| v.id);
        MonitorSnapshot { tasks }
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
    let limit = entry.task.spec().probe().limit();
    limit > 0 && entry.task.metrics().executions >= limit
}

/// Надёжный сток: единственный потребитель record-канала.
/// Если внешний sink задан — шлём туда с backpressure, иначе просто разгребаем.
async fn log_writer(
    mut rx: mpsc::Receiver<MonitorEvent>,
    hist_tx: Option<mpsc::Sender<MonitorEvent>>,
) {
    while let Some(ev) = rx.recv().await {
        if let Some(tx) = &hist_tx {
            if tx.send(ev).await.is_err() {
                tracing::warn!("history sink closed, log writer stops");
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct OrchestratorHandle {
    cmd_tx: mpsc::Sender<Command>,
    live_tx: broadcast::Sender<MonitorEvent>,
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
        self.live_tx.subscribe()
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
            probe::{
                ProbeError, ProbeExecution, ProbeOutcome, ProbeSpec, ProbeSpecError, SnmpGetSpec,
                SnmpSetSpec,
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
        let probe = ProbeSpec::snmp_get(request, Duration::from_secs(1), limit, attempt).unwrap();
        let task = Task::new(task_id, TaskConfig::try_new("test".to_string(), probe, 3).unwrap());
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
                worker: Some(WorkerHandle {
                    generation: Generation::first(),
                    abort: join.abort_handle(),
                    cmd,
                }),
            },
        );
        (orchestrator, events, task_id)
    }

    fn execution(at: i64, success: bool) -> ProbeExecution {
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
        ProbeExecution {
            finished_at,
            elapsed: Duration::from_millis(if success { 50 } else { 80 }),
            outcome: ProbeOutcome::SnmpGet(response),
        }
    }

    async fn execute(orchestrator: &mut Orchestrator, task_id: TaskId, execution: ProbeExecution) {
        orchestrator
            .on_report(WorkerReport::Executed {
                task_id,
                generation: Generation::first(),
                execution,
            })
            .await;
    }

    #[tokio::test]
    async fn no_response_is_an_execution_and_does_not_restart_the_worker() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        let first = execution(100, false);
        execute(&mut orchestrator, task_id, first.clone()).await;

        let event = events.try_recv().unwrap();
        let EventData::ProbeExecuted { execution, metrics } = event.data else {
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
        assert!(orchestrator.entries[&task_id].worker.is_some());

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
        assert!(matches!(executed.data, EventData::ProbeExecuted { .. }));
        assert!(matches!(completed.data, EventData::TaskCompleted));
        assert_eq!(completed.metadata.seq, executed.metadata.seq + 1);
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Completed);
        assert_eq!(snapshot.tasks[0].metrics.executions, 1);
        assert!(snapshot.tasks[0].result.is_some());
        assert!(orchestrator.entries[&task_id].worker.is_none());
    }

    #[tokio::test]
    async fn execution_from_another_generation_is_ignored() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(WorkerReport::Executed {
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
    async fn execution_error_preserves_its_type_and_restarts_the_worker() {
        let (mut orchestrator, mut events, task_id) = active_task(0);
        orchestrator
            .on_report(WorkerReport::ExecutionFailed {
                task_id,
                generation: Generation::first(),
                error: ProbeError::Polling(FatalError {
                    message: "invalid request".to_string(),
                }),
            })
            .await;

        assert!(matches!(
            events.try_recv().unwrap().data,
            EventData::ProbeError {
                error: ProbeError::Polling(FatalError { message }),
                retry_in: Some(_),
            } if message == "invalid request"
        ));
        let snapshot = orchestrator.snapshot();
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
        assert_eq!(snapshot.tasks[0].metrics.executions, 0);
        assert!(snapshot.tasks[0].result.is_none());
        assert!(orchestrator.entries[&task_id].worker.is_none());
    }

    #[tokio::test]
    async fn scenario_conversion_failure_is_reported_and_schedules_restart() {
        let get =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let set = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "private".to_string(),
            None,
            vec![],
        )
        .unwrap();
        let attempt = AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap();
        for probe in [
            ProbeSpec::SnmpGet {
                request: get,
                interval: Duration::ZERO,
                limit: 0,
                attempt,
            },
            ProbeSpec::SnmpSet {
                request: set,
                interval: Duration::ZERO,
                limit: 0,
                attempt,
            },
        ] {
            let (mut orchestrator, mut events, task_id) = active_task(0);
            orchestrator.stop(task_id).await.unwrap();
            let config = TaskConfig::try_new("invalid interval".to_string(), probe, 3).unwrap();
            orchestrator.update(task_id, config).await.unwrap();
            orchestrator.start(task_id).await.unwrap();
            assert!(matches!(events.try_recv().unwrap().data, EventData::TaskStopped));
            assert!(matches!(events.try_recv().unwrap().data, EventData::TaskSpecUpdated));
            let starting = events.try_recv().unwrap();
            assert!(matches!(starting.data, EventData::TaskStarting { attempt: 0 }));
            let report =
                tokio::time::timeout(Duration::from_secs(1), orchestrator.report_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            let reason = ProbeSpecError::IntervalMustBePositive.to_string();
            assert_eq!(report.key(), (task_id, starting.metadata.generation));
            assert!(
                matches!(&report, WorkerReport::BuildFailed { reason: actual, .. } if actual == &reason)
            );

            orchestrator.on_report(report).await;
            assert!(matches!(
                events.try_recv().unwrap().data,
                EventData::BuildFailed { reason: actual, retry_in: Some(_) } if actual == reason
            ));
            let snapshot = orchestrator.snapshot();
            assert_eq!(snapshot.tasks[0].name, "invalid interval");
            assert_eq!(snapshot.tasks[0].status, TaskStatus::Restarting);
            assert_eq!(snapshot.tasks[0].metrics.executions, 0);
            assert!(orchestrator.entries[&task_id].worker.is_none());
        }
    }
}
