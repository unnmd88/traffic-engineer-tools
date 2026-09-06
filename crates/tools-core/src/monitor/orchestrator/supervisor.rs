use std::{collections::HashMap, panic::AssertUnwindSafe, time::{Duration, Instant}};

use futures_util::FutureExt;
use tokio::sync::mpsc;

use crate::{
    monitor::{
        task::TaskId,
        usecase::{UseCase, UseCaseOutput},
    },
    polling::{
        Metrics, PollConfig,
        worker::{PollWorker, WorkerEvent, WorkerFinished, WorkerHandle, WorkerId},
    },
};

use super::restart_policy::RestartPolicy;

/// Воркер-менеджер: владеет хендлами воркеров и состоянием рестарта.
pub struct Supervisor {
    runtimes: HashMap<TaskId, WorkerRuntime>,
    policy: RestartPolicy,
    events_tx: mpsc::Sender<WorkerEvent<UseCaseOutput>>,
    exit_tx: mpsc::Sender<(TaskId, WorkerFinished)>,
    exit_rx: mpsc::Receiver<(TaskId, WorkerFinished)>,
}

struct WorkerRuntime {
    worker: Option<WorkerHandle>,
    restart: RestartState,
}

#[derive(Debug, Default)]
struct RestartState {
    attempts: u32,
    next_at: Option<Instant>,
}

impl Supervisor {
    pub fn new(events_tx: mpsc::Sender<WorkerEvent<UseCaseOutput>>) -> Self {
        let (exit_tx, exit_rx) = mpsc::channel(64);
        Self {
            runtimes: HashMap::new(),
            policy: RestartPolicy::default(),
            events_tx,
            exit_tx,
            exit_rx,
        }
    }

    pub fn spawn(
        &mut self,
        task_id: TaskId,
        use_case: UseCase,
        poll_config: PollConfig,
        metrics: Metrics,
    ) {
        let worker_id = WorkerId(task_id.0);
        let worker =
            PollWorker::new(worker_id, use_case, poll_config, self.events_tx.clone(), metrics);

        let exit_tx = self.exit_tx.clone();
        let join = tokio::spawn(async move {
            let finished = match AssertUnwindSafe(worker.run()).catch_unwind().await {
                Ok(finished) => finished,
                Err(panic) => WorkerFinished::Failed(format!("panic: {}", panic_message(panic))),
            };
            tracing::info!(target: "supervisor", task_id = %task_id, ?finished, "worker task finished");
            let _ = exit_tx.send((task_id, finished)).await;
        });

        let abort = join.abort_handle();
        drop(join); // detached: итог приходит через exit-канал

        self.runtimes.insert(
            task_id,
            WorkerRuntime {
                worker: Some(WorkerHandle::new(abort)),
                restart: RestartState::default(),
            },
        );
    }

    pub fn stop(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            if let Some(handle) = rt.worker.take() {
                handle.abort();
            }
        }
    }

    pub fn remove(&mut self, task_id: &TaskId) {
        self.runtimes.remove(task_id);
    }

    pub fn is_running(&self, task_id: &TaskId) -> bool {
        self.runtimes
            .get(task_id)
            .is_some_and(|rt| rt.worker.is_some())
    }

    /// Очередной исход завершения воркера (event-driven).
    pub async fn next_exit(&mut self) -> Option<(TaskId, WorkerFinished)> {
        self.exit_rx.recv().await
    }

    /// Воркер упал (Fatal/panic): пометить и запланировать рестарт.
    /// Возвращает (attempt, delay) для лога.
    pub fn schedule_restart(&mut self, task_id: &TaskId) -> Option<(u32, Duration)> {
        let rt = self.runtimes.get_mut(task_id)?;
        rt.worker = None;
        rt.restart.attempts += 1;
        let delay = self.policy.delay(rt.restart.attempts);
        rt.restart.next_at = Some(Instant::now() + delay);
        Some((rt.restart.attempts, delay))
    }

    /// Воркер завершился штатно (rate limit) — просто пометить остановленным.
    pub fn mark_stopped(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.worker = None;
        }
    }

    /// Сбросить счётчик/таймер при успешном перезапуске.
    pub fn reset_restart(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.restart = RestartState::default();
        }
    }

    /// Ошибка сборки при рестарте — отложить ещё раз.
    pub fn retry_later(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.restart.attempts += 1;
            let delay = self.policy.delay(rt.restart.attempts);
            rt.restart.next_at = Some(Instant::now() + delay);
        }
    }

    /// Кто готов к перезапуску (backoff истёк).
    pub fn due_restarts(&self, now: Instant) -> Vec<TaskId> {
        self.runtimes
            .iter()
            .filter(|(_, rt)| rt.restart.next_at.is_some_and(|t| t <= now))
            .map(|(id, _)| id.clone())
            .collect()
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
