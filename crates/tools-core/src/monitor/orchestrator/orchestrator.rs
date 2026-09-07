use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    monitor::{
        task::{
            MonitorSnapshot, PollStatus, TaskEntity, TaskId, TaskRepository, TaskSnapshot, TaskSpec,
            TaskView,
        },
        usecase::{UseCase, UseCaseOutput},
    },
    polling::worker::{WorkerEvent, WorkerFinished},
};

use super::{error::OrchestratorError, supervisor::Supervisor};

// Команды извне (Application/API)
pub enum OrchestratorCommand {
    AddTask {
        spec: TaskSpec,
        reply: oneshot::Sender<Result<TaskId, OrchestratorError>>,
    },
    RemoveTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<TaskEntity, OrchestratorError>>,
    },
    StartTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    StopTask(TaskId),
    UpdateTask {
        task_id: TaskId,
        spec: TaskSpec,
        reply: oneshot::Sender<Result<(), OrchestratorError>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<MonitorSnapshot>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<OrchestratorEvent>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

// События наружу (UI/API)
#[derive(Clone, Debug)]
pub enum OrchestratorEvent {
    /// Задача изменилась (добавлена / обновлена / изменён статус или результат).
    TaskUpdated { task_id: TaskId, view: TaskView },
    /// Задача удалена.
    TaskRemoved { task_id: TaskId },
}

/// Зачем запускается сборка адаптера: влияет на обработку ошибки сборки.
enum BuildIntent {
    /// Ручной `start_task`: при ошибке — возврат в Idle, без ретраев.
    Start,
    /// Пересоздание (update/restart): при ошибке — Restarting + backoff.
    Rebuild,
}

/// Исход сборки адаптера, приходящий из отдельной таски.
struct BuildOutcome {
    task_id: TaskId,
    intent: BuildIntent,
    generation: u64,
    result: Result<UseCase, OrchestratorError>,
}

pub struct Orchestrator {
    repository: TaskRepository,
    supervisor: Supervisor,
    cmd_rx: mpsc::Receiver<OrchestratorCommand>,
    events_rx: mpsc::Receiver<WorkerEvent<UseCaseOutput>>,
    broadcast_tx: broadcast::Sender<OrchestratorEvent>,
    supervisor_tick: tokio::time::Interval,
    build_tx: mpsc::Sender<BuildOutcome>,
    build_rx: mpsc::Receiver<BuildOutcome>,
    /// Сборки в полёте (не даём запускать дубликат на тот же task_id).
    pending_builds: HashSet<TaskId>,
    /// Поколение спеки: инкрементируется при update_spec, чтобы отбрасывать
    /// сборки, начатые до обновления (результат которых уже устарел).
    build_generation: HashMap<TaskId, u64>,
}

impl Orchestrator {
    pub fn new() -> (Self, OrchestratorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (events_tx, events_rx) = mpsc::channel::<WorkerEvent<UseCaseOutput>>(32);
        let (build_tx, build_rx) = mpsc::channel::<BuildOutcome>(32);
        let (broadcast_tx, _) = broadcast::channel(16);
        (
            Self {
                repository: TaskRepository::new_empty(),
                supervisor: Supervisor::new(events_tx),
                cmd_rx,
                events_rx,
                broadcast_tx,
                supervisor_tick: tokio::time::interval(Duration::from_secs(1)),
                build_tx,
                build_rx,
                pending_builds: HashSet::new(),
                build_generation: HashMap::new(),
            },
            OrchestratorHandle { cmd_tx },
        )
    }

    #[tracing::instrument(name = "orchestrator", skip_all)]
    pub async fn run(mut self) {
        tracing::info!("orchestrator started");
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    if !self.handle_command(cmd) {
                        break;
                    }
                }
                Some(ev)  = self.events_rx.recv() => self.handle_worker_event(ev),
                Some(outcome) = self.build_rx.recv() => self.handle_build_outcome(outcome),
                exit = self.supervisor.next_exit() => {
                    if let Some((task_id, finished)) = exit {
                        self.handle_worker_exit(task_id, finished);
                    }
                }
                _ = self.supervisor_tick.tick() => self.supervise_due_restarts(),
            }
        }
        tracing::info!("orchestrator stopped");
    }

    /// Возвращает `false`, когда оркестратор должен завершиться (Shutdown).
    fn handle_command(&mut self, cmd: OrchestratorCommand) -> bool {
        match cmd {
            OrchestratorCommand::AddTask { spec, reply } => {
                tracing::info!(name = %spec.name(), target = %spec.query().target(), "command: add_task");
                let task_id = self.add_task(spec);
                self.broadcast_update(task_id);
                let _ = reply.send(Ok(task_id));
            }
            OrchestratorCommand::RemoveTask { task_id, reply } => {
                tracing::info!(task_id = %task_id, "command: remove_task");
                let result = self.remove_task(&task_id);
                if result.is_ok() {
                    self.broadcast_removed(task_id);
                }
                let _ = reply.send(result);
            }
            OrchestratorCommand::StartTask { task_id, reply } => {
                tracing::info!(task_id = %task_id, "command: start_task");
                let _ = reply.send(self.start_task(&task_id));
            }
            OrchestratorCommand::StopTask(task_id) => {
                tracing::info!(task_id = %task_id, "command: stop_task");
                self.stop_task(&task_id);
            }
            OrchestratorCommand::UpdateTask {
                task_id,
                spec,
                reply,
            } => {
                tracing::info!(task_id = %task_id, name = ?spec, "command: update_task");
                let _ = reply.send(self.update_task(&task_id, spec));
            }
            OrchestratorCommand::GetSnapshot { reply } => {
                tracing::info!("command: get_snapshot");
                let _ = reply.send(self.repository.snapshot());
            }
            OrchestratorCommand::Subscribe { reply } => {
                tracing::info!("command: subscribe");
                let _ = reply.send(self.broadcast_tx.subscribe());
            }
            OrchestratorCommand::Shutdown { reply } => {
                tracing::info!("command: shutdown");
                self.supervisor.stop_all();
                let _ = reply.send(());
                return false;
            }
        }
        true
    }

    fn add_task(&mut self, spec: TaskSpec) -> TaskId {
        self.repository.add_task(spec)
    }

    fn start_task(&mut self, task_id: &TaskId) -> Result<(), OrchestratorError> {
        if self.supervisor.is_running(task_id) || self.pending_builds.contains(task_id) {
            return Ok(()); // уже запущена или сборка в полёте
        }
        if self.repository.get_task(task_id).is_none() {
            return Err(OrchestratorError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        }

        // Явный ручной старт — сбрасываем накопленный backoff.
        self.supervisor.reset_restart(task_id);
        // Новая сессия: сбрасываем счётчики, чтобы `limit` считался за запуск.
        let _ = self.repository.reset_metrics(task_id);
        self.schedule_build(*task_id, BuildIntent::Start);
        Ok(())
    }

    fn stop_task(&mut self, task_id: &TaskId) {
        self.supervisor.stop(task_id);
        self.set_status(task_id, PollStatus::Paused);
        self.broadcast_update(*task_id);
    }

    fn update_task(&mut self, task_id: &TaskId, spec: TaskSpec) -> Result<(), OrchestratorError> {
        if self.repository.get_task(task_id).is_none() {
            return Err(OrchestratorError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        }

        self.repository.update_spec(task_id, spec)?;
        // Инвалидируем сборки, начатые до обновления спеки.
        *self.build_generation.entry(*task_id).or_insert(0) += 1;

        if self.supervisor.is_running(task_id) {
            self.supervisor.stop(task_id);
            // Сборка асинхронная; воркер появится, когда придёт BuildOutcome.
            self.schedule_build(*task_id, BuildIntent::Rebuild);
        }

        self.broadcast_update(*task_id);
        Ok(())
    }

    fn remove_task(&mut self, task_id: &TaskId) -> Result<TaskEntity, OrchestratorError> {
        self.supervisor.stop(task_id);
        self.supervisor.remove(task_id);
        self.pending_builds.remove(task_id);
        self.build_generation.remove(task_id);
        self.repository.remove_task(task_id).map_err(Into::into)
    }

    /// Единственная точка входа для запроса сборки адаптера: спавнит build-таску
    /// вне цикла и НЕ трогает воркера — воркер появится в [`handle_build_outcome`].
    /// Вызывается из `start_task` / `update_task` / `restart_task` и при пересборке
    /// устаревшего результата. Возвращает `false`, если задача не найдена или
    /// сборка уже в полёте.
    fn schedule_build(&mut self, task_id: TaskId, intent: BuildIntent) -> bool {
        if self.pending_builds.contains(&task_id) {
            tracing::debug!(task_id = %task_id, "build already in flight, skipping");
            return false;
        }

        let Some((query, attempt)) = self
            .repository
            .get_task(&task_id)
            .map(|t| (t.spec().query().clone(), t.spec().poll_config().attempt()))
        else {
            tracing::warn!(task_id = %task_id, "cannot schedule build: task not found");
            return false;
        };

        let generation = self.build_generation.get(&task_id).copied().unwrap_or(0);
        self.pending_builds.insert(task_id);
        // Таймер рестарта израсходован: воркер появится после сборки.
        self.supervisor.mark_building(&task_id);

        let build_tx = self.build_tx.clone();
        tokio::spawn(async move {
            let result = UseCase::build(query, attempt).await.map_err(Into::into);
            if build_tx
                .send(BuildOutcome {
                    task_id,
                    intent,
                    generation,
                    result,
                })
                .await
                .is_err()
            {
                tracing::warn!(task_id = %task_id, "build outcome receiver dropped");
            }
        });

        true
    }

    /// Обрабатывает результат сборки из build-канала. Единственный вызыватель
    /// [`spawn_worker`] (а значит, и `Supervisor::spawn`). Отбрасывает сборку,
    /// начатую до изменения спеки (по `generation`), и по `intent` решает, что
    /// делать с ошибкой сборки: `Start` → `Idle`, `Rebuild` → `Restarting` + retry.
    fn handle_build_outcome(&mut self, outcome: BuildOutcome) {
        let BuildOutcome {
            task_id,
            intent,
            generation,
            result,
        } = outcome;
        self.pending_builds.remove(&task_id);

        // Спека изменилась, пока шла сборка → результат устарел, пересобираем.
        let current_generation = self.build_generation.get(&task_id).copied().unwrap_or(0);
        if generation != current_generation {
            tracing::warn!(
                task_id = %task_id,
                generation,
                current_generation,
                "stale build discarded; rescheduling"
            );
            self.schedule_build(task_id, intent);
            return;
        }

        match result {
            Ok(use_case) => {
                tracing::info!(task_id = %task_id, "adapter built, spawning worker");
                self.spawn_worker(task_id, use_case);
                self.set_status(&task_id, PollStatus::Active);
                self.broadcast_update(task_id);
            }
            Err(e) => match intent {
                BuildIntent::Start => {
                    tracing::warn!(task_id = %task_id, error = %e, "build failed on manual start");
                    self.set_status(&task_id, PollStatus::Idle);
                    self.broadcast_update(task_id);
                }
                BuildIntent::Rebuild => {
                    tracing::warn!(task_id = %task_id, error = %e, "build failed on rebuild; will retry");
                    self.supervisor.retry_later(&task_id);
                    self.set_status(&task_id, PollStatus::Restarting);
                    self.broadcast_update(task_id);
                }
            },
        }
    }

    /// Единственный вызыватель `Supervisor::spawn`. Читает `poll_config` и
    /// seed-метрики из репо и передаёт их супервизору. Вызывается только из
    /// [`handle_build_outcome`] после успешной сборки адаптера.
    fn spawn_worker(&mut self, task_id: TaskId, use_case: UseCase) {
        let Some((poll_config, metrics)) = self
            .repository
            .get_task(&task_id)
            .map(|t| (*t.poll_config(), *t.metrics()))
        else {
            return;
        };
        self.supervisor
            .spawn(task_id, use_case, poll_config, metrics);
    }

    fn set_status(&mut self, task_id: &TaskId, status: PollStatus) {
        let _ = self.repository.update_status(task_id, status);
    }

    fn handle_worker_event(&mut self, event: WorkerEvent<UseCaseOutput>) {
        let task_id = TaskId(event.id.0);
        let Some(task) = self.repository.get_task(&task_id) else {
            tracing::warn!(worker_id = ?event.id, "task for worker not found");
            return;
        };

        // Воркер пережил хотя бы один опрос (событие приходит только после
        // успешного poll) — сбрасываем backoff.
        self.supervisor.reset_restart(&task_id);

        let limit = task.poll_config().limit();
        let status = if limit > 0 && event.metrics.total_attempts >= limit {
            PollStatus::RatedLimit
        } else {
            PollStatus::Active
        };

        let snapshot = TaskSnapshot::new()
            .with_poll_result(event.result)
            .with_poll_status(status)
            .with_metrics(event.metrics);

        if self.repository.update_snapshot(&task_id, snapshot).is_ok() {
            self.broadcast_update(task_id);
        }
    }

    fn broadcast_update(&mut self, task_id: TaskId) {
        if self.broadcast_tx.receiver_count() == 0 {
            return;
        }
        let Some(task) = self.repository.get_task(&task_id) else {
            return; // задача удалена — для этого есть broadcast_removed
        };
        let view = TaskView::from(task);
        let _ = self
            .broadcast_tx
            .send(OrchestratorEvent::TaskUpdated { task_id, view });
    }

    fn broadcast_removed(&mut self, task_id: TaskId) {
        if self.broadcast_tx.receiver_count() > 0 {
            let _ = self
                .broadcast_tx
                .send(OrchestratorEvent::TaskRemoved { task_id });
        }
    }

    fn handle_worker_exit(&mut self, task_id: TaskId, finished: WorkerFinished) {
        match finished {
            WorkerFinished::Completed => {
                self.supervisor.mark_stopped(&task_id);
                self.set_status(&task_id, PollStatus::RatedLimit);
                self.broadcast_update(task_id);
                tracing::info!(task_id = %task_id, "worker completed");
            }
            WorkerFinished::Failed(message) => {
                if self.supervisor.schedule_restart(&task_id).is_none() {
                    return;
                }
                self.set_status(&task_id, PollStatus::Restarting);
                self.broadcast_update(task_id);
                tracing::error!(task_id = %task_id, "worker failed ({message})");
            }
        }
    }

    fn supervise_due_restarts(&mut self) {
        let due = self.supervisor.due_restarts(Instant::now());
        for task_id in due {
            self.restart_task(task_id);
        }
    }

    fn restart_task(&mut self, task_id: TaskId) {
        // Сборка адаптера — в отдельной таске; воркер появится по BuildOutcome.
        self.schedule_build(task_id, BuildIntent::Rebuild);
    }
}

#[derive(Clone)]
pub struct OrchestratorHandle {
    cmd_tx: mpsc::Sender<OrchestratorCommand>,
}

impl OrchestratorHandle {
    pub async fn add_task(&self, spec: TaskSpec) -> Result<TaskId, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::AddTask { spec, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<TaskEntity, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::RemoveTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::StartTask { task_id, reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)?
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.cmd_tx
            .send(OrchestratorCommand::StopTask(task_id))
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskSpec,
    ) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::UpdateTask {
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
            .send(OrchestratorCommand::GetSnapshot { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub async fn subscribe(
        &self,
    ) -> Result<broadcast::Receiver<OrchestratorEvent>, OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::Subscribe { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }

    /// Остановить оркестратор: гасит все воркеры и завершает цикл обработки.
    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(OrchestratorCommand::Shutdown { reply: tx })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)?;
        rx.await.map_err(|_| OrchestratorError::ChannelClosed)
    }
}
