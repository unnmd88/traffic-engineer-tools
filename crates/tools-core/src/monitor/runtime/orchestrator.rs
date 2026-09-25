use std::collections::HashMap;

use chrono::Local;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::{
    monitor::{
        event::{ChangeKind, MonitorEvent},
        runtime::{
            error::OrchestratorError,
            restart_policy::RestartPolicy,
            worker::{Generation, Worker, WorkerCmd, WorkerReport},
        },
        task::{HistoryEntryView, MonitorSnapshot, Task, TaskConfig, TaskId, TaskStatus, TaskView},
    },
    polling::Response,
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
    record_tx: mpsc::Sender<MonitorEvent>,
    policy: RestartPolicy,
}

impl Orchestrator {
    pub fn new(hist_tx: Option<mpsc::Sender<MonitorEvent>>) -> (Self, OrchestratorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (report_tx, report_rx) = mpsc::channel(256);
        let (record_tx, record_rx) = mpsc::channel(1024);
        let (live_tx, _) = broadcast::channel(1024);

        tokio::spawn(log_writer(record_rx, hist_tx));

        let this = Self {
            next_id: 1,
            entries: HashMap::new(),
            cmd_rx,
            cmd_tx: cmd_tx.clone(),
            report_rx,
            report_tx,
            live_tx: live_tx.clone(),
            record_tx,
            policy: RestartPolicy::default(),
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
                        worker: None,
                    },
                );
                self.emit(id, ChangeKind::Added).await;
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
                self.maybe_respawn(task_id);
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
        self.emit(id, ChangeKind::Starting).await;
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
            self.emit(id, ChangeKind::Paused).await;
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
            self.emit(id, ChangeKind::Stopped).await;
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

        self.emit(id, ChangeKind::SpecUpdated).await;

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
        let view = self.view(&entry);
        self.emit_event(MonitorEvent::TaskRemoved {
            task_id: id,
            view,
            at: Local::now(),
        })
        .await;
        Ok(())
    }

    async fn on_report(&mut self, rep: WorkerReport) {
        let (task_id, generation) = rep.key();

        let (kind, respawn_after): (Option<ChangeKind>, Option<Duration>) = {
            let Some(entry) = self.entries.get_mut(&task_id) else {
                return;
            };
            if entry.worker.as_ref().map(|w| w.generation) != Some(generation) {
                return;
            }

            match rep {
                WorkerReport::Built { .. } => {
                    let was_restart = entry.attempts > 0;
                    entry.attempts = 0;
                    entry.state = TaskState::Active;
                    let kind = if was_restart {
                        ChangeKind::Restarted
                    } else {
                        ChangeKind::Started
                    };
                    (Some(kind), None)
                }
                WorkerReport::BuildFailed { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (Some(ChangeKind::BuildFailed { reason }), Some(delay))
                }
                WorkerReport::BuildTimeout { .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (
                        Some(ChangeKind::BuildFailed {
                            reason: "build timeout".to_string(),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::BuildPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (
                        Some(ChangeKind::Panicked {
                            reason: format!("build: {reason}"),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::Polled { response, .. } => {
                    let new_metrics = match &response {
                        Response::Success { elapsed, .. } => {
                            entry.task.metrics().with_success(*elapsed)
                        }
                        Response::NoResponse { .. } => entry.task.metrics().with_error(),
                    };
                    entry.task.apply_poll(response, new_metrics);
                    if limit_reached(entry) {
                        if let Some(w) = entry.worker.take() {
                            w.abort.abort();
                        }
                        entry.state = TaskState::Completed;
                        (Some(ChangeKind::Completed), None)
                    } else {
                        (Some(ChangeKind::Polled), None)
                    }
                }
                WorkerReport::PollFailed { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (Some(ChangeKind::Failed { reason }), Some(delay))
                }
                WorkerReport::PollPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (
                        Some(ChangeKind::Panicked {
                            reason: format!("poll: {reason}"),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::WorkerPanicked { reason, .. } => {
                    entry.worker = None;
                    entry.attempts += 1;
                    entry.state = TaskState::Restarting;
                    let delay = self.policy.delay(entry.attempts);
                    (
                        Some(ChangeKind::Panicked {
                            reason: format!("worker: {reason}"),
                        }),
                        Some(delay),
                    )
                }
                WorkerReport::Paused { .. } => {
                    entry.state = TaskState::Paused;
                    (Some(ChangeKind::Paused), None)
                }
                WorkerReport::Resumed { .. } => {
                    entry.state = TaskState::Active;
                    (Some(ChangeKind::Resumed), None)
                }
            }
        };

        if let Some(kind) = kind {
            self.emit(task_id, kind).await;
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
        let query = entry.task.spec().query().clone();
        let poll = entry.task.spec().poll_config();
        let build_timeout = poll.attempt().budget();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let worker = Worker::new(
            task_id,
            generation,
            query,
            poll,
            build_timeout,
            self.report_tx.clone(),
            cmd_rx,
        );
        let join = tokio::spawn(worker.run());
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

    fn maybe_respawn(&mut self, task_id: TaskId) {
        let should = matches!(
            self.entries.get(&task_id),
            Some(e) if e.state == TaskState::Restarting && e.worker.is_none()
        );
        if should {
            {
                let entry = self.entries.get_mut(&task_id).expect("checked above");
                entry.state = TaskState::Starting;
            }
            self.spawn_worker(task_id);
        }
    }

    async fn emit(&mut self, task_id: TaskId, kind: ChangeKind) {
        let view = match self.entries.get(&task_id) {
            Some(entry) => self.view(entry),
            None => return,
        };
        self.emit_event(MonitorEvent::TaskChanged {
            task_id,
            kind,
            view,
            at: Local::now(),
        })
        .await;
    }

    async fn emit_event(&mut self, ev: MonitorEvent) {
        // live: fire-and-forget, отстающие отваливаются сами
        if self.live_tx.receiver_count() > 0 {
            let _ = self.live_tx.send(ev.clone());
        }
        // запись: backpressure — не теряем события
        let _ = self.record_tx.send(ev).await;
    }

    fn view(&self, entry: &TaskEntry) -> TaskView {
        let t = &entry.task;
        let spec = t.spec();
        let history = t
            .history()
            .iter()
            .map(|h| HistoryEntryView {
                timestamp: h.timestamp,
                result: Some(h.result.clone()),
            })
            .collect();

        TaskView {
            id: t.id(),
            name: spec.name().to_string(),
            revision: spec.revision(),
            target: spec.query().target(),
            status: status(entry),
            interval: spec.poll_config().interval(),
            limit: spec.poll_config().limit(),
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
    let limit = entry.task.spec().poll_config().limit();
    limit > 0 && entry.task.metrics().total_attempts >= limit
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
