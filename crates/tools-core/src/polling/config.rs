use tokio::time::Duration;

/// Расписание и лимит одного воркера.
#[derive(Clone, Copy, Debug, Default)]
pub struct PollConfig {
    /// Период между началами опросов.
    pub interval: Duration,
    /// Максимум итераций опроса за жизнь задачи (0 — без лимита).
    pub limit: u64,
    /// Параметры одной итерации опроса.
    pub attempt: AttemptConfig,
}

/// Параметры одной итерации опроса: сколько раз стучаться и как ждать.
#[derive(Clone, Copy, Debug, Default)]
pub struct AttemptConfig {
    /// Таймаут одной попытки.
    pub timeout: Duration,
    /// Число повторов ПОСЛЕ первой попытки (итого попыток = 1 + retries).
    pub retries: u8,
    /// Пауза между попытками.
    pub retry_delay: Duration,
}
