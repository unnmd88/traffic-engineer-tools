use derive_more::Display;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    error::Error,
    monitor::{
        TaskRepository,
        application::{Orchestrator, OrchestratorEvent, OrchestratorHandle, config::AppConfig},
        task::TaskId,
    },
};

#[derive(Clone, Display)]
pub struct ApplicationId(Uuid);

impl ApplicationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Display, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationState {
    Idle,
    Runnig,
}

pub struct Application {
    id: ApplicationId,
    state: ApplicationState,
    handle: OrchestratorHandle,
    task_ids: Vec<TaskId>,
}

impl Application {
    pub async fn new(config: AppConfig) -> Result<Self, Error> {
        let (orchestrator, handle) = Orchestrator::new();
        tokio::spawn(orchestrator.run());

        let mut task_ids = Vec::new();
        for task in config.tasks {
            let spec = task.try_into()?;
            let id = handle.add_task(spec).await?;
            task_ids.push(id);
        }

        Ok(Self {
            id: ApplicationId::generate(),
            state: ApplicationState::Idle,
            handle,
            task_ids,
        })
    }

    pub async fn start(&mut self) -> ApplicationState {
        if matches!(self.state, ApplicationState::Idle) {
            for id in &self.task_ids {
                let _ = self.handle.start_task(*id).await;
            }
            self.state = ApplicationState::Runnig;
        }
        self.state
    }

    pub fn current_state(&self) -> ApplicationState {
        self.state
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, ApplicationState::Runnig)
    }

    pub async fn get_snapshot(&self) -> Result<TaskRepository, Error> {
        Ok(self.handle.get_snapshot().await?)
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<OrchestratorEvent>, Error> {
        Ok(self.handle.subscribe().await?)
    }

    pub fn id(&self) -> &ApplicationId {
        &self.id
    }
}
