use std::future::Future;

use chrono::Utc;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{AbortHandle, JoinSet},
};
use tracing::Instrument;

use super::{Command, Message};
use crate::monitor::{
    EventData, Generation, RestartPlan, ScenarioFailure, TaskCommandKind, TaskConfig, TaskId,
    TaskProgress, TaskSnapshot, TaskStatus, TaskTransition, TaskTransitionCause,
    scenario::{RunMode, ScenarioContext, ScenarioExit},
    task::Task,
};

pub(super) struct Entry {
    pub task: Task,
    pub desired: RunMode,
    pub execution: Execution,
    pub generation: Option<Generation>,
    pub attempts: u32,
    pub progress: Option<TaskProgress>,
    pub last_failure: Option<ScenarioFailure>,
    last_transition: Option<TaskTransition>,
}

impl Entry {
    pub fn new(id: TaskId, config: TaskConfig) -> Self {
        Self {
            task: Task::new(id, config),
            desired: RunMode::Running,
            execution: Execution::Dormant,
            generation: None,
            attempts: 0,
            progress: None,
            last_failure: None,
            last_transition: None,
        }
    }

    pub fn status(&self) -> TaskStatus {
        self.last_transition
            .as_ref()
            .map_or(TaskStatus::Idle, |transition| transition.current)
    }

    pub fn can_start(&self) -> bool {
        matches!(self.status(), TaskStatus::Idle | TaskStatus::Stopped | TaskStatus::Completed)
    }

    pub fn is_live(&self, generation: Generation) -> bool {
        self.generation == Some(generation) && matches!(self.execution, Execution::Live(_))
    }

    pub fn cancel_timer(&mut self) {
        if let Execution::Backoff(timer) = &self.execution {
            timer.abort();
            self.execution = Execution::Dormant;
        }
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        self.task.snapshot(
            self.status(),
            self.progress.as_ref(),
            self.last_transition.as_ref(),
            self.last_failure.as_ref(),
        )
    }

    pub fn transition(
        &mut self,
        current: TaskStatus,
        cause: TaskTransitionCause,
        restart: Option<RestartPlan>,
    ) -> Option<EventData> {
        let previous = self.status();
        if previous == current {
            return None;
        }
        let run_limit = match cause {
            TaskTransitionCause::InitialStart
            | TaskTransitionCause::Command(TaskCommandKind::Start) => self.task.run_limit(),
            _ => None,
        };
        let transition = TaskTransition {
            previous,
            current,
            cause,
            restart,
            run_limit,
            at: Utc::now(),
        };
        self.progress = None;
        self.last_transition = Some(transition.clone());
        Some(EventData::TaskStateChanged(Box::new(transition)))
    }
}

#[derive(Default)]
pub(super) enum Execution {
    #[default]
    Dormant,
    Backoff(AbortHandle),
    Live(Box<LiveRun>),
}

pub(super) struct LiveRun {
    pub mode: watch::Sender<RunMode>,
    cancel: Option<oneshot::Sender<()>>,
    pub pending: Vec<Command>,
}

impl LiveRun {
    /// Posts the exit only after dropping the scenario future, including on cancellation.
    pub fn spawn<F: Future<Output = ScenarioExit> + Send + 'static>(
        workers: &mut JoinSet<()>,
        tx: mpsc::Sender<Message>,
        context: ScenarioContext,
        mode: watch::Sender<RunMode>,
        execute: impl FnOnce(oneshot::Receiver<()>) -> F,
    ) -> Self {
        let (cancel, stop) = oneshot::channel();
        let future = execute(stop);
        let span = tracing::info_span!("scenario", task_id = %context.task_id, generation = ?context.generation, revision = context.revision.0);
        workers.spawn(
            async move {
                let outcome = future.await;
                let _ = tx.send(Message::Exited(context, outcome)).await;
            }
            .instrument(span),
        );
        Self {
            mode,
            cancel: Some(cancel),
            pending: Vec::new(),
        }
    }

    pub fn is_closing(&self) -> bool {
        self.cancel.is_none()
    }

    pub fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}
