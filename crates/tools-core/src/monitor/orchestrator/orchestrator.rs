use std::{sync::Arc, time::{Duration, Instant}};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    error::OrchestratorError,
    monitor::{
        task::{PollStatus, TaskEntity, TaskId, TaskRepository, TaskSnapshot, TaskSpec},
        usecase::{UseCase, UseCaseOutput},
    },
    polling::worker::{WorkerEvent, WorkerFinished},
};

use super::supervisor::Supervisor;

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
        reply: oneshot::Sender<TaskRepository>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<OrchestratorEvent>>,
    },
}

// События наружу (UI/API)
#[derive(Clone, Debug)]
pub enum OrchestratorEvent {
    Update {
        snapshot: Arc<TaskRepository>,
        task_id: TaskId,
    },
}

pub struct Orchestrator {
    repository: TaskRepository,
    supervisor: Supervisor,
    cmd_rx: mpsc::Receiver<OrchestratorCommand>,
    events_rx: mpsc::Receiver<WorkerEvent<UseCaseOutput>>,
    broadcast_tx: broadcast::Sender<OrchestratorEvent>,
    supervisor_tick: tokio::time::Interval,
}

impl Orchestrator {
    pub fn new() -> (Self, OrchestratorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (events_tx, events_rx) = mpsc::channel::<WorkerEvent<UseCaseOutput>>(32);
        let (broadcast_tx, _) = broadcast::channel(16);
        (
            Self {
                repository: TaskRepository::new_empty(),
                supervisor: Supervisor::new(events_tx),
                cmd_rx,
                events_rx,
                broadcast_tx,
                supervisor_tick: tokio::time::interval(Duration::from_secs(1)),
            },
            OrchestratorHandle { cmd_tx },
        )
    }

    #[tracing::instrument(name = "orchestrator", skip_all)]
    pub async fn run(mut self) {
        tracing::info!("orchestrator started");
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => self.handle_command(cmd).await,
                Some(ev)  = self.events_rx.recv() => self.handle_worker_event(ev),
                exit = self.supervisor.next_exit() => {
                    if let Some((task_id, finished)) = exit {
                        self.handle_worker_exit(task_id, finished);
                    }
                }
                _ = self.supervisor_tick.tick() => self.supervise_due_restarts().await,
            }
        }
    }

    async fn handle_command(&mut self, cmd: OrchestratorCommand) {
        match cmd {
            OrchestratorCommand::AddTask { spec, reply } => {
                let _ = reply.send(self.add_task(spec).await);
            }
            OrchestratorCommand::RemoveTask { task_id, reply } => {
                let _ = reply.send(self.remove_task(&task_id));
            }
            OrchestratorCommand::StartTask { task_id, reply } => {
                let _ = reply.send(self.start_task(&task_id).await);
            }
            OrchestratorCommand::StopTask(task_id) => {
                self.stop_task(&task_id).await;
            }
            OrchestratorCommand::UpdateTask { task_id, spec, reply } => {
                let _ = reply.send(self.update_task(&task_id, spec).await);
            }
            OrchestratorCommand::GetSnapshot { reply } => {
                let _ = reply.send(self.repository.clone());
            }
            OrchestratorCommand::Subscribe { reply } => {
                let _ = reply.send(self.broadcast_tx.subscribe());
            }
        }
    }

    async fn add_task(&mut self, spec: TaskSpec) -> Result<TaskId, OrchestratorError> {
        Ok(self.repository.add_task(spec))
    }

    async fn start_task(&mut self, task_id: &TaskId) -> Result<(), OrchestratorError> {
        if self.supervisor.is_running(task_id) {
            return Ok(());
        }

        let use_case = self.build_use_case(task_id).await?;
        self.spawn_worker(task_id.clone(), use_case);
        self.set_status(task_id, PollStatus::Active);
        self.broadcast_update(task_id.clone());
        Ok(())
    }

    async fn stop_task(&mut self, task_id: &TaskId) {
        self.supervisor.stop(task_id);
        self.set_status(task_id, PollStatus::Paused);
        self.broadcast_update(task_id.clone());
    }

    async fn update_task(
        &mut self,
        task_id: &TaskId,
        spec: TaskSpec,
    ) -> Result<(), OrchestratorError> {
        if self.repository.get_task(task_id).is_none() {
            return Err(OrchestratorError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        }

        self.repository.update_spec(task_id, spec)?;

        if self.supervisor.is_running(task_id) {
            self.supervisor.stop(task_id);
            match self.build_use_case(task_id).await {
                Ok(use_case) => self.spawn_worker(task_id.clone(), use_case),
                Err(e) => {
                    tracing::warn!(task_id = %task_id, error = %e, "rebuild after update failed");
                }
            }
        }

        self.broadcast_update(task_id.clone());
        Ok(())
    }

    fn remove_task(&mut self, task_id: &TaskId) -> Result<TaskEntity, OrchestratorError> {
        self.supervisor.stop(task_id);
        self.supervisor.remove(task_id);
        self.repository.remove_task(task_id).map_err(Into::into)
    }

    async fn build_use_case(&self, task_id: &TaskId) -> Result<UseCase, OrchestratorError> {
        let Some(task) = self.repository.get_task(task_id) else {
            return Err(OrchestratorError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        };
        let spec = task.spec().clone();
        UseCase::build(spec.query.clone(), spec.poll_config.attempt)
            .await
            .map_err(Into::into)
    }

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

        let limit = task.poll_config().limit;
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
        if self.broadcast_tx.receiver_count() > 0 {
            let _ = self.broadcast_tx.send(OrchestratorEvent::Update {
                snapshot: Arc::new(self.repository.clone()),
                task_id,
            });
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
                let Some((attempt, delay)) = self.supervisor.schedule_restart(&task_id) else {
                    return;
                };
                self.set_status(&task_id, PollStatus::Restarting);
                self.broadcast_update(task_id.clone());
                tracing::error!(
                    task_id = %task_id,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "worker failed ({message}), scheduling restart"
                );
            }
        }
    }

    async fn supervise_due_restarts(&mut self) {
        let due = self.supervisor.due_restarts(Instant::now());
        for task_id in due {
            self.restart_task(task_id).await;
        }
    }

    async fn restart_task(&mut self, task_id: TaskId) {
        self.supervisor.reset_restart(&task_id);

        match self.build_use_case(&task_id).await {
            Ok(use_case) => {
                self.spawn_worker(task_id.clone(), use_case);
                self.set_status(&task_id, PollStatus::Active);
                self.broadcast_update(task_id);
            }
            Err(e) => {
                tracing::warn!(task_id = %task_id, error = %e, "rebuild failed, will retry");
                self.supervisor.retry_later(&task_id);
            }
        }
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

    pub async fn get_snapshot(&self) -> Result<TaskRepository, OrchestratorError> {
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
}
