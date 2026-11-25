use tokio::sync::{broadcast, mpsc, oneshot};

use crate::monitor::{
    Generation, MonitorCommandError, MonitorEvent, MonitorSnapshot, MonitorSubscription, RunLimit,
    TaskConfig, TaskId, channel::ChannelSender,
};

pub(super) enum Command {
    AddTask {
        spec: TaskConfig,
        reply: oneshot::Sender<Result<TaskId, MonitorCommandError>>,
    },
    RemoveTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
    },
    StartTask {
        task_id: TaskId,
        limit: RunLimit,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
    },
    PauseTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
    },
    ResumeTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
    },
    StopTask {
        task_id: TaskId,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
    },
    UpdateTask {
        task_id: TaskId,
        spec: TaskConfig,
        reply: oneshot::Sender<Result<(), MonitorCommandError>>,
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

#[derive(Clone)]
enum CommandSender {
    Current(mpsc::Sender<Command>),
    Ordered(ChannelSender<super::ordered::QueuedMessage>),
}

impl CommandSender {
    async fn send(&self, command: Command) -> Result<(), MonitorCommandError> {
        match self {
            Self::Current(sender) => sender
                .send(command)
                .await
                .map_err(|_| MonitorCommandError::ChannelClosed),
            Self::Ordered(sender) => sender
                .send_while_running(command)
                .await
                .map_err(|_| MonitorCommandError::ChannelClosed),
        }
    }
}

#[derive(Clone)]
pub(super) struct OrchestratorHandle {
    cmd_tx: CommandSender,
    live_tx: broadcast::WeakSender<MonitorEvent>,
}

impl OrchestratorHandle {
    pub(super) fn current(
        sender: mpsc::Sender<Command>,
        live_tx: broadcast::WeakSender<MonitorEvent>,
    ) -> Self {
        Self {
            cmd_tx: CommandSender::Current(sender),
            live_tx,
        }
    }

    pub(super) fn ordered(
        sender: ChannelSender<super::ordered::QueuedMessage>,
        live_tx: broadcast::WeakSender<MonitorEvent>,
    ) -> Self {
        Self {
            cmd_tx: CommandSender::Ordered(sender),
            live_tx,
        }
    }

    pub async fn add_task(&self, spec: TaskConfig) -> Result<TaskId, MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddTask { spec, reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn remove_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::RemoveTask { task_id, reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn start_task(
        &self,
        task_id: TaskId,
        limit: RunLimit,
    ) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StartTask {
                task_id,
                limit,
                reply: tx,
            })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn pause_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::PauseTask { task_id, reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn resume_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::ResumeTask { task_id, reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn stop_task(&self, task_id: TaskId) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StopTask { task_id, reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn update_task(
        &self,
        task_id: TaskId,
        spec: TaskConfig,
    ) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::UpdateTask {
                task_id,
                spec,
                reply: tx,
            })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)?
    }

    pub async fn get_snapshot(&self) -> Result<MonitorSnapshot, MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(Command::GetSnapshot { reply: tx }).await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)
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

    pub async fn subscribe_with_snapshot(
        &self,
    ) -> Result<MonitorSubscription, MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubscribeWithSnapshot { reply: tx })
            .await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)
    }

    pub async fn shutdown(&self) -> Result<(), MonitorCommandError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(Command::Shutdown { reply: tx }).await?;
        rx.await.map_err(|_| MonitorCommandError::ChannelClosed)
    }
}
