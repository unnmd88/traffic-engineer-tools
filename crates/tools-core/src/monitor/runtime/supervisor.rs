use std::collections::HashMap;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::monitor::{
    event::TaskFact,
    runtime::{
        error::SupervisorError,
        task_actor::{ActorCommand, Desired, TaskActor},
    },
    task::{TaskConfig, TaskId},
};

/// Сообщение наблюдателя: актор завершился (паника → reason = Some).
struct Death {
    id: TaskId,
    reason: Option<String>,
}

/// Хэндл актора с точки зрения супервизора: как послать команду и как прибить.
pub struct ActorHandle {
    mailbox: mpsc::Sender<ActorCommand>,
    abort: tokio::task::AbortHandle,
}

enum Command {
    AddTask {
        spec: TaskConfig,
        reply: oneshot::Sender<Result<TaskId, SupervisorError>>,
    },
    RemoveTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), SupervisorError>>,
    },
    StartTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), SupervisorError>>,
    },
    StopTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), SupervisorError>>,
    },
    UpdateTask {
        task_id: TaskId,
        spec: TaskConfig,
        reply: oneshot::Sender<Result<(), SupervisorError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Супервизор — источник истины: какие задачи есть, с какой спекой, запущены или нет.
/// Акторы — воркеры; при падении супервизор пересоздаёт их по сохранённой спеке.
pub struct Supervisor {
    next_id: u64,
    specs: HashMap<TaskId, (TaskConfig, Desired)>,
    actors: HashMap<TaskId, ActorHandle>,
    cmd_rx: mpsc::Receiver<Command>,
    death_rx: mpsc::Receiver<Death>,
    death_tx: mpsc::Sender<Death>,
    fact_tx: mpsc::Sender<TaskFact>,
}

impl Supervisor {
    pub fn new(fact_tx: mpsc::Sender<TaskFact>) -> (Self, SupervisorHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (death_tx, death_rx) = mpsc::channel(256);
        let this = Self {
            next_id: 1,
            specs: HashMap::new(),
            actors: HashMap::new(),
            cmd_rx,
            death_rx,
            death_tx,
            fact_tx,
        };
        (this, SupervisorHandle { cmd_tx })
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    if !self.on_command(cmd).await {
                        break;
                    }
                }
                Some(death) = self.death_rx.recv() => self.on_death(death).await,
                else => break,
            }
        }
    }

    async fn on_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::AddTask { spec, reply } => {
                let id = TaskId(self.next_id);
                self.next_id += 1;
                self.spawn_actor(id, spec.clone(), Desired::Stopped);
                self.specs.insert(id, (spec, Desired::Stopped));
                let _ = reply.send(Ok(id));
                true
            }
            Command::RemoveTask { task_id, reply } => {
                let res = self.remove(task_id).await;
                let _ = reply.send(res);
                true
            }
            Command::StartTask { task_id, reply } => {
                let res = self
                    .set_desired_and_forward(task_id, Desired::Running, ActorCommand::Start)
                    .await;
                let _ = reply.send(res);
                true
            }
            Command::StopTask { task_id, reply } => {
                let res = self
                    .set_desired_and_forward(task_id, Desired::Stopped, ActorCommand::Stop)
                    .await;
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
            Command::Shutdown { reply } => {
                for actor in self.actors.values() {
                    actor.abort.abort();
                }
                self.actors.clear();
                self.specs.clear();
                let _ = reply.send(());
                false
            }
        }
    }

    async fn on_death(&mut self, death: Death) {
        let Some((spec, desired)) = self.specs.get(&death.id) else {
            // задача уже удалена — просто подчистить хэндл
            self.actors.remove(&death.id);
            return;
        };
        let (spec, desired) = (spec.clone(), *desired);

        if desired == Desired::Running {
            // упала работающая задача: сообщаем проектору и пересоздаём (спека жива здесь)
            if let Some(reason) = &death.reason {
                let _ = self
                    .fact_tx
                    .send(TaskFact::Crashed {
                        task_id: death.id,
                        reason: reason.clone(),
                    })
                    .await;
            }
            self.spawn_actor(death.id, spec, Desired::Running);
        } else {
            // остановленная задача завершилась штатно (Shutdown) — убрать хэндл
            self.actors.remove(&death.id);
        }
    }

    async fn set_desired_and_forward(
        &mut self,
        id: TaskId,
        desired: Desired,
        cmd: ActorCommand,
    ) -> Result<(), SupervisorError> {
        let Some((_, d)) = self.specs.get_mut(&id) else {
            return Err(SupervisorError::TaskNotFound(id));
        };
        *d = desired;
        self.forward(id, cmd).await
    }

    async fn update(&mut self, id: TaskId, spec: TaskConfig) -> Result<(), SupervisorError> {
        let Some((stored, _)) = self.specs.get_mut(&id) else {
            return Err(SupervisorError::TaskNotFound(id));
        };
        *stored = spec.clone();
        self.forward(id, ActorCommand::Update { spec }).await
    }

    async fn remove(&mut self, id: TaskId) -> Result<(), SupervisorError> {
        let Some(actor) = self.actors.remove(&id) else {
            return Err(SupervisorError::TaskNotFound(id));
        };
        actor.abort.abort();
        self.specs.remove(&id);
        let _ = self.fact_tx.send(TaskFact::Removed { task_id: id }).await;
        Ok(())
    }

    async fn forward(&self, id: TaskId, cmd: ActorCommand) -> Result<(), SupervisorError> {
        let Some(actor) = self.actors.get(&id) else {
            return Err(SupervisorError::TaskNotFound(id));
        };
        actor
            .mailbox
            .send(cmd)
            .await
            .map_err(|_| SupervisorError::ChannelClosed)
    }

    fn spawn_actor(&mut self, id: TaskId, spec: TaskConfig, desired: Desired) {
        let (actor, mailbox) = TaskActor::new(id, spec, desired, self.fact_tx.clone());
        let join = tokio::spawn(actor.run());
        let abort = join.abort_handle();
        self.actors.insert(id, ActorHandle { mailbox, abort });

        // наблюдатель: дождётся ЛЮБОГО конца (в т.ч. abort) и сообщит id + причину паники
        let death_tx = self.death_tx.clone();
        tokio::spawn(async move {
            let reason = join
                .await
                .err()
                .and_then(|e| e.try_into_panic().ok())
                .map(panic_message);
            let _ = death_tx.send(Death { id, reason }).await;
        });
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

#[derive(Clone)]
pub struct SupervisorHandle {
    cmd_tx: mpsc::Sender<Command>,
}

impl SupervisorHandle {
    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddTask { spec, reply: tx })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)?
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::RemoveTask { task_id, reply: tx })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)?
    }

    pub async fn start_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StartTask { task_id, reply: tx })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)?
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StopTask { task_id, reply: tx })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)?
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::UpdateTask {
                task_id,
                spec,
                reply: tx,
            })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)?
    }

    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Shutdown { reply: tx })
            .await
            .map_err(|_| SupervisorError::ChannelClosed)?;
        rx.await.map_err(|_| SupervisorError::ChannelClosed)
    }
}
