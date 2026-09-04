use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    error::OrchestratorError,
    monitor::{
        TaskRepository,
        application::{UseCase, config::TaskSpec},
        task::{PollStatus, TaskEntity, TaskId, TaskSnapshot, TaskUpdateDto},
    },
    polling::AttemptConfig,
    polling::worker::{
        PollWorker, WorkerCommand, WorkerEvent, WorkerHandle, WorkerId, WorkerState,
    },
};

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
    StartTask(TaskId),
    StopTask(TaskId),
    SetLimit {
        task_id: TaskId,
        limit: u64,
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

#[derive(Debug)]
pub struct TaskWorker {
    worker_id: WorkerId,
    worker_control: WorkerHandle,
}

pub struct Orchestrator {
    repository: TaskRepository,
    workers: HashMap<TaskId, TaskWorker>,
    worker_to_task: HashMap<WorkerId, TaskId>,
    specs: HashMap<TaskId, TaskSpec>,
    cmd_rx: mpsc::Receiver<OrchestratorCommand>,
    events_tx: mpsc::Sender<WorkerEvent>,
    events_rx: mpsc::Receiver<WorkerEvent>,
    broadcast_tx: broadcast::Sender<OrchestratorEvent>,
}

impl Orchestrator {
    pub fn new() -> (Self, OrchestratorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (events_tx, events_rx) = mpsc::channel(32);
        let (broadcast_tx, _) = broadcast::channel(16);
        (
            Self {
                repository: TaskRepository::new_empty(),
                workers: HashMap::new(),
                worker_to_task: HashMap::new(),
                specs: HashMap::new(),
                cmd_rx,
                events_tx,
                events_rx,
                broadcast_tx,
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
            }
        }
    }

    async fn handle_command(&mut self, cmd: OrchestratorCommand) {
        match cmd {
            OrchestratorCommand::AddTask { spec, reply } => {
                let result = self.add_task(spec).await;
                let _ = reply.send(result);
            }

            OrchestratorCommand::RemoveTask { task_id, reply } => {
                let _ = reply.send(self.remove_task(&task_id));
            }
            OrchestratorCommand::StartTask(id) => {
                self.send_to_worker(&id, WorkerCommand::Start).await
            }
            OrchestratorCommand::StopTask(id) => {
                self.send_to_worker(&id, WorkerCommand::Stop).await
            }
            OrchestratorCommand::SetLimit { task_id, limit } => {
                self.send_to_worker(&task_id, WorkerCommand::SetLimit(limit))
                    .await
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
        let attempt: AttemptConfig = spec.poll_config.attempt.clone().into();
        let use_case = UseCase::build(spec.query.clone(), attempt).await?;

        let task_id =
            self.repository
                .add_task(spec.meta.clone(), Some(spec.poll_config.clone()), None, None);

        let worker_id = WorkerId(task_id.0);
        let handle = PollWorker::spawn(
            worker_id,
            use_case,
            spec.poll_config.clone().into(), // TaskPollConfig -> PollConfig
            self.events_tx.clone(),
        );

        self.worker_to_task.insert(worker_id, task_id.clone());
        self.workers.insert(
            task_id.clone(),
            TaskWorker {
                worker_id,
                worker_control: handle,
            },
        );
        self.specs.insert(task_id.clone(), spec);

        Ok(task_id)
    }

    fn remove_task(&mut self, task_id: &TaskId) -> Result<TaskEntity, OrchestratorError> {
        if let Some(tw) = self.workers.remove(task_id) {
            tw.worker_control.abort();
            self.worker_to_task.remove(&tw.worker_id);
        }
        self.repository.remove_task(task_id).map_err(Into::into)
    }

    async fn send_to_worker(&mut self, task_id: &TaskId, cmd: WorkerCommand) {
        match self.workers.get(task_id) {
            Some(tw) => {
                if let Err(e) = tw.worker_control.send(cmd).await {
                    tracing::warn!(task_id = %task_id, error = %e, "send command failed");
                }
            }
            None => tracing::warn!(task_id = %task_id, "task not found"),
        }
    }

    fn handle_worker_event(&mut self, event: WorkerEvent) {
        let Some(task_id) = self.worker_to_task.get(&event.id).cloned() else {
            tracing::warn!(worker_id = ?event.id, "task for worker not found");
            return;
        };

        let snapshot = TaskSnapshot::new()
            .with_poll_result(event.poll_result)
            .with_poll_status(event.state.into())
            .with_metrics(event.metrics);

        let update = TaskUpdateDto {
            snapshot: Some(snapshot),
            poll_config: Some(event.poll_config.into()), // PollConfig -> TaskPollConfig
        };

        if self.repository.update_task(&task_id, update).is_ok()
            && self.broadcast_tx.receiver_count() > 0
        {
            let _ = self.broadcast_tx.send(OrchestratorEvent::Update {
                snapshot: Arc::new(self.repository.clone()),
                task_id,
            });
        }
    }
}

impl From<WorkerState> for PollStatus {
    fn from(state: WorkerState) -> Self {
        match state {
            WorkerState::Idle => Self::Idle,
            WorkerState::Running => Self::Active,
            WorkerState::Stopped => Self::Paused,
            WorkerState::RatedLimit => Self::RateLimit,
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
        self.cmd_tx
            .send(OrchestratorCommand::StartTask(task_id))
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), OrchestratorError> {
        self.cmd_tx
            .send(OrchestratorCommand::StopTask(task_id))
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
    }

    pub async fn set_limit(&self, task_id: TaskId, limit: u64) -> Result<(), OrchestratorError> {
        self.cmd_tx
            .send(OrchestratorCommand::SetLimit { task_id, limit })
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
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
