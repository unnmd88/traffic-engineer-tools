use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

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

#[derive(Default)]
struct WorkerRuntime {
    worker: Option<WorkerHandle>,
    restart: RestartState,
}

#[derive(Debug, Default)]
struct RestartState {
    attempts: u32,
    next_at: Option<Instant>,
}

/// Воркер-пул: владеет хендлами воркеров и состоянием рестарта (control-часть).
/// Это пассивная структура — её дёргает `Orchestrator` из `apply`, своего цикла у неё нет.
pub struct WorkersPool {
    runtimes: HashMap<TaskId, WorkerRuntime>,
    policy: RestartPolicy,

    worker_event_tx: mpsc::Sender<WorkerEvent<UseCaseOutput>>,
    worker_exit_tx: mpsc::Sender<(TaskId, WorkerFinished)>,
}

impl WorkersPool {
    pub fn new(
        worker_event_tx: mpsc::Sender<WorkerEvent<UseCaseOutput>>,
        worker_exit_tx: mpsc::Sender<(TaskId, WorkerFinished)>,
    ) -> Self {
        Self {
            runtimes: HashMap::new(),
            policy: RestartPolicy::default(),
            worker_event_tx,
            worker_exit_tx,
        }
    }

    /// Единственная точка запуска воркера: создаёт `PollWorker`, оборачивает его
    /// `run` в `catch_unwind` (паника → `WorkerFinished::Failed`) и сохраняет
    /// `abort`-хендл.
    #[tracing::instrument(name = "workers_pool", skip_all, fields(task_id = %task_id))]
    pub fn spawn(
        &mut self,
        task_id: TaskId,
        use_case: UseCase,
        poll_config: PollConfig,
        metrics: Metrics,
    ) {
        tracing::info!("worker spawned");
        let worker_id = WorkerId(task_id.0);
        let worker =
            PollWorker::new(worker_id, use_case, poll_config, self.worker_event_tx.clone(), metrics);

        let exit_tx = self.worker_exit_tx.clone();
        let join = tokio::spawn(async move {
            let finished = match AssertUnwindSafe(worker.run()).catch_unwind().await {
                Ok(finished) => finished,
                Err(panic) => WorkerFinished::Failed(format!("panic: {}", panic_message(panic))),
            };
            tracing::info!(target: "workers_pool", task_id = %task_id, ?finished, "worker task finished");
            let _ = exit_tx.send((task_id, finished)).await;
        });

        let abort = join.abort_handle();
        drop(join); // detached: итог приходит через exit-канал

        let rt = self.runtimes.entry(task_id).or_default();
        // Защита от утечки: если по этому task_id уже жил воркер — гасим его.
        if let Some(previous) = rt.worker.take() {
            previous.abort();
            tracing::warn!("replacing a live worker (previous one aborted)");
        }
        rt.worker = Some(WorkerHandle::new(abort));
        // Таймер рестарта израсходован; attempts НЕ сбрасываем, чтобы backoff нарастал,
        // если новый воркер упадёт до первого успешного опроса.
        rt.restart.next_at = None;
    }

    #[tracing::instrument(name = "workers_pool", skip_all, fields(task_id = %task_id))]
    pub fn stop(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            if let Some(handle) = rt.worker.take() {
                handle.abort();
                tracing::info!("worker stopped");
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

    /// Воркер упал (Fatal/panic): пометить и запланировать рестарт.
    #[tracing::instrument(name = "workers_pool", skip_all, fields(task_id = %task_id))]
    pub fn schedule_restart(&mut self, task_id: &TaskId) -> Option<(u32, Duration)> {
        let rt = self.runtimes.get_mut(task_id)?;
        rt.worker = None;
        rt.restart.attempts += 1;
        let delay = self.policy.delay(rt.restart.attempts);
        rt.restart.next_at = Some(Instant::now() + delay);
        tracing::info!(
            attempt = rt.restart.attempts,
            delay_ms = delay.as_millis() as u64,
            "restart scheduled"
        );
        Some((rt.restart.attempts, delay))
    }

    /// Воркер завершился штатно (rate limit) — просто пометить остановленным.
    #[tracing::instrument(name = "workers_pool", skip_all, fields(task_id = %task_id))]
    pub fn mark_stopped(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.worker = None;
            tracing::info!("worker marked stopped");
        }
    }

    /// Сбросить счётчик/таймер backoff после успешного опроса или ручного старта.
    pub fn reset_restart(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.restart = RestartState::default();
        }
    }

    /// Сборка адаптера в полёте — снимаем отложенный таймер рестарта.
    pub fn mark_building(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.restart.next_at = None;
        }
    }

    /// Ошибка сборки при рестарте — отложить ещё раз.
    #[tracing::instrument(name = "workers_pool", skip_all, fields(task_id = %task_id))]
    pub fn retry_later(&mut self, task_id: &TaskId) {
        if let Some(rt) = self.runtimes.get_mut(task_id) {
            rt.restart.attempts += 1;
            let delay = self.policy.delay(rt.restart.attempts);
            rt.restart.next_at = Some(Instant::now() + delay);
            tracing::warn!(
                attempt = rt.restart.attempts,
                delay_ms = delay.as_millis() as u64,
                "restart postponed"
            );
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

    /// Остановить всех воркеров и сбросить отложенные рестарты (graceful shutdown).
    pub fn stop_all(&mut self) {
        for rt in self.runtimes.values_mut() {
            if let Some(handle) = rt.worker.take() {
                handle.abort();
            }
            rt.restart = RestartState::default();
        }
    }
}

impl Drop for WorkersPool {
    fn drop(&mut self) {
        // Отвязанные воркеры живут на рантайме независимо от WorkersPool,
        // поэтому при дропе обязательно гасим все хендлы.
        self.stop_all();
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
