use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until};

use crate::monitor::event::{ChangeKind, TaskFact};
use crate::{
    monitor::{
        adapter::Adapter,
        runtime::restart_policy::RestartPolicy,
        task::{HistoryEntryView, Task, TaskConfig, TaskId, TaskStatus, TaskView},
    },
    polling::{Response, poll},
};

/// Желание пользователя: запущено / остановлено.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Desired {
    Running,
    Stopped,
}

/// Реальность: что делает актор сейчас. Без отдельного воркера —
/// актор сам опрашивает, поэтому нет `Polling { handle }`.
enum Runtime {
    Idle,
    Polling,
    BackingOff { until: Instant },
    Exhausted,
}

/// Команда извне (супервизор пересылает сюда).
pub enum ActorCommand {
    Start,
    Stop,
    Update { spec: TaskConfig },
    Shutdown,
}

/// Актор одной задачи: владеет спекой (копией) и runtime-данными,
/// пересобирает адаптер, ловит панику опроса/сборки, шлёт факты в проектор.
pub struct TaskActor {
    mailbox: mpsc::Receiver<ActorCommand>,
    fact_tx: mpsc::Sender<TaskFact>,

    data: Task,
    desired: Desired,
    runtime: Runtime,
    attempts: u32,
    ever_started: bool,
    use_case: Option<Adapter>,
    policy: RestartPolicy,
}

impl TaskActor {
    pub fn new(
        id: TaskId,
        spec: TaskConfig,
        desired: Desired,
        fact_tx: mpsc::Sender<TaskFact>,
    ) -> (Self, mpsc::Sender<ActorCommand>) {
        let (mailbox_tx, mailbox_rx) = mpsc::channel(32);
        let actor = Self {
            mailbox: mailbox_rx,
            fact_tx,
            data: Task::new(id, spec),
            desired,
            runtime: Runtime::Idle,
            attempts: 0,
            ever_started: desired == Desired::Running,
            use_case: None,
            policy: RestartPolicy::default(),
        };
        (actor, mailbox_tx)
    }

    pub async fn run(mut self) {
        // зарегистрироваться в проекторе (view строится из спеки; runtime-данные пусты)
        self.emit(ChangeKind::Added).await;

        loop {
            // 1) слить очередь команд (несколько Update схлопываются: last-wins)
            while let Ok(cmd) = self.mailbox.try_recv() {
                if !self.handle(cmd).await {
                    return; // Shutdown
                }
            }

            // 2) выбрать фазу по (desired, runtime)
            let keep_going = match (&self.desired, &self.runtime) {
                (Desired::Stopped, _) | (Desired::Running, Runtime::Exhausted) => {
                    self.wait_cmd().await
                }
                (Desired::Running, Runtime::Idle) => {
                    self.build().await;
                    true
                }
                (Desired::Running, Runtime::Polling) => self.poll_cycle().await,
                (Desired::Running, Runtime::BackingOff { .. }) => self.backoff().await,
            };
            if !keep_going {
                return;
            }
        }
    }

    /// Ждём команду (не опрашиваем).
    async fn wait_cmd(&mut self) -> bool {
        match self.mailbox.recv().await {
            Some(cmd) => self.handle(cmd).await,
            None => false, // mailbox закрыт — завершаемся
        }
    }

    /// Собрать адаптер. Слушаем команды параллельно: если пришёл Update/Stop,
    /// сборка отменяется, и цикл пересоберёт по новой спеке.
    async fn build(&mut self) -> bool {
        let spec = self.data.spec();
        let query = spec.query().clone();
        let attempt = spec.poll_config().attempt();

        tokio::select! {
            cmd = self.mailbox.recv() => match cmd {
                Some(c) => self.handle(c).await,
                None => false,
            },
            res = Adapter::build(query, attempt) => match res {
                Ok(uc) => {
                    let was_restart = self.attempts > 0;
                    self.use_case = Some(uc);
                    self.attempts = 0;
                    self.runtime = Runtime::Polling;
                    self.emit(if was_restart { ChangeKind::Restarted } else { ChangeKind::Started }).await;
                    true
                }
                Err(err) => {
                    self.attempts += 1;
                    let until = Instant::now() + self.policy.delay(self.attempts);
                    self.runtime = Runtime::BackingOff { until };
                    self.emit(ChangeKind::BuildFailed { reason: err.to_string() }).await;
                    true
                }
            },
        }
    }

    /// Фаза опроса: ждём либо команду, либо пора опрашивать.
    async fn poll_cycle(&mut self) -> bool {
        let interval = self.data.spec().poll_config().interval();
        tokio::select! {
            cmd = self.mailbox.recv() => match cmd {
                Some(c) => self.handle(c).await,
                None => false,
            },
            _ = sleep(interval) => {
                self.do_poll().await;
                true
            }
        }
    }

    /// Один опрос. Паника адаптера == фатальная ошибка.
    async fn do_poll(&mut self) {
        let attempt = self.data.spec().poll_config().attempt();
        let outcome = AssertUnwindSafe(poll(&attempt, self.use_case.as_ref().unwrap()))
            .catch_unwind()
            .await;

        match outcome {
            Ok(Ok(response)) => {
                let new_metrics = match &response {
                    Response::Success { elapsed, .. } => self.data.metrics().with_success(*elapsed),
                    Response::NoResponse { .. } => self.data.metrics().with_error(),
                };
                self.attempts = 0;
                self.data.apply_poll(response, new_metrics);
                if self.limit_reached() {
                    self.runtime = Runtime::Exhausted;
                    self.emit(ChangeKind::Completed).await;
                } else {
                    self.emit(ChangeKind::Polled).await;
                }
            }
            Ok(Err(fatal)) => {
                self.fail(format!("poll failed: {}", fatal.message)).await;
            }
            Err(panic) => {
                self.fail(format!("panic: {}", panic_message(panic))).await;
            }
        }
    }

    async fn fail(&mut self, reason: String) {
        self.attempts += 1;
        self.use_case = None; // заставит пересобрать
        let until = Instant::now() + self.policy.delay(self.attempts);
        self.runtime = Runtime::BackingOff { until };
        self.emit(ChangeKind::Failed { reason }).await;
    }

    /// Фаза backoff: ждём таймер или команду.
    async fn backoff(&mut self) -> bool {
        let until = match &self.runtime {
            Runtime::BackingOff { until } => *until,
            _ => return true,
        };
        tokio::select! {
            cmd = self.mailbox.recv() => match cmd {
                Some(c) => self.handle(c).await,
                None => false,
            },
            _ = sleep_until(until) => {
                self.runtime = Runtime::Idle; // цикл перейдёт в build
                true
            }
        }
    }

    /// Команда: меняет оси, не трогает опрос напрямую. `false` = Shutdown.
    async fn handle(&mut self, cmd: ActorCommand) -> bool {
        match cmd {
            ActorCommand::Start => {
                if self.desired == Desired::Running && !matches!(self.runtime, Runtime::Exhausted) {
                    return true; // уже работает — no-op
                }
                self.desired = Desired::Running;
                self.ever_started = true;
                self.attempts = 0;
                self.data.reset_run();
                self.runtime = Runtime::Idle;
                self.emit(ChangeKind::Starting).await;
                true
            }
            ActorCommand::Stop => {
                self.desired = Desired::Stopped;
                self.attempts = 0;
                self.use_case = None; // освободить адаптер
                self.runtime = Runtime::Idle;
                self.emit(ChangeKind::Stopped).await;
                true
            }
            ActorCommand::Update { spec } => {
                let was_running = self.desired == Desired::Running;
                self.data.update_spec(spec);
                self.use_case = None;
                self.attempts = 0;
                if was_running {
                    self.runtime = Runtime::Idle;
                }
                self.emit(ChangeKind::SpecUpdated).await;
                true
            }
            ActorCommand::Shutdown => false,
        }
    }

    fn limit_reached(&self) -> bool {
        let limit = self.data.spec().poll_config().limit();
        limit > 0 && self.data.metrics().total_attempts >= limit
    }

    /// Статус — проекция осей, не хранится.
    fn status(&self) -> TaskStatus {
        use Desired::*;
        use Runtime::*;
        match (&self.desired, &self.runtime) {
            (Stopped, _) => {
                if self.ever_started {
                    TaskStatus::Stopped
                } else {
                    TaskStatus::Idle
                }
            }
            (Running, Idle) => TaskStatus::Starting,
            (Running, Polling) => TaskStatus::Active,
            (Running, BackingOff { .. }) => TaskStatus::Restarting,
            (Running, Exhausted) => TaskStatus::Completed,
        }
    }

    fn view(&self) -> TaskView {
        let t = &self.data;
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
            target: spec.query().target().clone(),
            status: self.status(),
            interval: spec.poll_config().interval(),
            limit: spec.poll_config().limit(),
            metrics: t.metrics(),
            result: t.last_result().cloned(),
            history,
            health: t.health().clone(),
        }
    }

    async fn emit(&self, kind: ChangeKind) {
        let view = self.view();
        if self
            .fact_tx
            .send(TaskFact::Changed {
                task_id: self.data.id(),
                kind,
                view,
            })
            .await
            .is_err()
        {
            tracing::warn!("projector closed, dropping fact");
        }
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
