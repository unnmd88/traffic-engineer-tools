use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::{
    monitor::{
        probe::ProbeOutput,
        runtime::worker::Generation,
        task::{SpecRevision, TaskId},
    },
    polling::{Metrics, PollErrorContext, Response},
};

/// Id команды: назначается ядром при создании команды.
/// Прокидывается в события как `correlation_id` (корневая причина цепочки).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommandId(pub u64);

/// Непосредственная причина события: команда пользователя или предыдущее событие.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Command(CommandId),
    /// `seq` предыдущего события.
    Event(u64),
}

/// Общая мета события: порядок и корреляция. Одинакова для всех фактов.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventMeta {
    /// Монотонный номер события внутри монитора — для порядка и реплея.
    pub seq: u64,
    /// Момент факта (UTC).
    pub at: DateTime<Utc>,
    /// Кто эмитит (id монитора/приложения).
    pub source: String,
    /// Про кого событие.
    pub task_id: TaskId,
    /// Запуск задачи (эпоха воркера).
    pub generation: Generation,
    /// Версия конфига задачи на момент факта.
    pub revision: SpecRevision,
    /// Корневая причина — команда, породившая цепочку (опционально).
    pub correlation_id: Option<CommandId>,
    /// Непосредственный родитель (опционально).
    pub causation: Option<Cause>,
}

/// Типизированный факт: что именно произошло. Past-tense, неизменяемый.
/// Растёт по мере появления режимов/протоколов — `#[non_exhaustive]`.
///
/// Намеренно без `PartialEq`: payload (`Response<ProbeOutput>`, `Metrics`) пока
/// не сравниваемы. Если понадобится для тестов — добавим `PartialEq` точечно.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum EventData {
    TaskAdded,
    TaskSpecUpdated,
    /// `attempt` > 0 — это рестарт после сбоя.
    TaskStarting { attempt: u32 },
    TaskStarted,
    TaskPaused,
    TaskResumed,
    TaskStopped,
    TaskCompleted,
    TaskRemoved,

    PollSucceeded {
        response: Response<ProbeOutput>,
        metrics: Metrics,
    },
    PollNoResponse {
        attempts: u8,
        errors: Vec<PollErrorContext>,
        elapsed: Duration,
        metrics: Metrics,
        consecutive_no_response: u32,
    },

    BuildFailed {
        reason: String,
        /// `Some` — будет рестарт через это время; `None` — ретраев больше не будет.
        retry_in: Option<Duration>,
    },
    PollError {
        reason: String,
        retry_in: Option<Duration>,
    },
    WorkerPanicked {
        reason: String,
        retry_in: Option<Duration>,
    },
}

/// Единое доменное событие наружу. Уходит в live-канал (broadcast) и durable-канал (mpsc).
#[derive(Clone, Debug)]
pub struct MonitorEvent {
    pub metadata: EventMeta,
    pub data: EventData,
}
