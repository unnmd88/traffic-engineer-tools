use std::{net::IpAddr, panic::AssertUnwindSafe};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::{
    sync::{mpsc, watch},
    time::{Duration, Instant},
};

use crate::{
    monitor::{OperationContext, OperationId, RunLimit, SpecRevision, TaskProgress, task::TaskId},
    polling::{AttemptConfig, AttemptError, PollProgress, PollReport, Pollable, poll_observed},
    snmp::{SnmpClient, SnmpClientConfig, SnmpError, community::Community},
};

use super::{
    Generation, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError, ScenarioSpec,
    schedule::PollTimer,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::monitor) struct ScenarioContext {
    pub task_id: TaskId,
    pub generation: Generation,
    pub revision: SpecRevision,
}

impl ScenarioContext {
    pub(super) fn update(self, data: UpdateData) -> ScenarioUpdate {
        ScenarioUpdate {
            task_id: self.task_id,
            generation: self.generation,
            data,
        }
    }
}

pub(in crate::monitor) enum RunBudget {
    Unlimited,
    Remaining(u64),
}

impl RunBudget {
    pub(in crate::monitor) const fn new(limit: RunLimit, completed_runs: u64) -> Self {
        match limit {
            RunLimit::Unlimited => Self::Unlimited,
            RunLimit::Runs(limit) => Self::Remaining(limit.get().saturating_sub(completed_runs)),
        }
    }

    pub(in crate::monitor) const fn is_exhausted(&self) -> bool {
        matches!(self, Self::Remaining(0))
    }

    pub(super) fn record_finished_run(&mut self) -> bool {
        if let Self::Remaining(remaining) = self {
            *remaining -= 1;
        }
        self.is_exhausted()
    }
}

pub(in crate::monitor) async fn run(
    spec: ScenarioSpec,
    context: ScenarioContext,
    budget: RunBudget,
    report_tx: mpsc::Sender<ScenarioReport>,
    mode_rx: watch::Receiver<RunMode>,
) {
    let channels = ScenarioChannels {
        reports: report_tx.clone(),
        mode: mode_rx,
    };
    let outcome = execute(spec, context, budget, channels, std::future::pending()).await;
    if let Some(report) = outcome.into_report(context) {
        let _ = report_tx.send(report).await;
    }
}

/// The outcome returned after the local scenario future has released its resources.
pub(in crate::monitor) enum ScenarioExit {
    /// Carries the final counted run at the limit, or no run for independent scenario completion.
    Completed(Option<ScenarioExecution>),
    PreparationFailed(ScenarioBuildError),
    ExecutionFailed(ScenarioExecutionError, Box<ScenarioExecution>),
    Panicked {
        stage: super::PanicStage,
        reason: String,
    },
    Cancelled,
}

impl ScenarioExit {
    fn into_report(self, context: ScenarioContext) -> Option<ScenarioReport> {
        use super::PanicStage;

        let ScenarioContext {
            task_id,
            generation,
            ..
        } = context;
        Some(match self {
            Self::Cancelled => return None,
            Self::Completed(None) => ScenarioReport::Completed {
                task_id,
                generation,
            },
            Self::Completed(Some(execution)) => ScenarioReport::RunFinished {
                task_id,
                generation,
                execution,
                limit_reached: true,
            },
            Self::PreparationFailed(error) => ScenarioReport::BuildFailed {
                task_id,
                generation,
                error,
            },
            Self::ExecutionFailed(error, execution) => ScenarioReport::ExecutionFailed {
                task_id,
                generation,
                error,
                execution: *execution,
            },
            Self::Panicked { stage, reason } => match stage {
                PanicStage::Build => ScenarioReport::BuildPanicked {
                    task_id,
                    generation,
                    reason,
                },
                PanicStage::Execution => ScenarioReport::ExecutionPanicked {
                    task_id,
                    generation,
                    reason,
                },
                PanicStage::Runtime => ScenarioReport::Panicked {
                    task_id,
                    generation,
                    reason,
                },
            },
        })
    }

    pub(super) fn build_failed(error: ScenarioBuildError) -> Self {
        match error {
            ScenarioBuildError::Panicked(reason) => {
                Self::panicked(super::PanicStage::Build, reason)
            }
            error => Self::PreparationFailed(error),
        }
    }

    pub(super) fn panicked(stage: super::PanicStage, reason: String) -> Self {
        Self::Panicked { stage, reason }
    }
}

pub(in crate::monitor) struct ScenarioChannels<M> {
    pub reports: mpsc::Sender<M>,
    pub mode: watch::Receiver<RunMode>,
}

/// Keeps a finished operation until the channel has accepted it, including across cancellation.
pub(super) struct RunReports<M> {
    pub(super) sender: mpsc::Sender<M>,
    pending: Option<ScenarioExecution>,
}

impl<M: From<ScenarioUpdate>> RunReports<M> {
    pub(super) async fn finished(
        &mut self,
        context: ScenarioContext,
        execution: ScenarioExecution,
    ) -> bool {
        self.pending = Some(execution);
        self.flush(context).await
    }

    async fn flush(&mut self, context: ScenarioContext) -> bool {
        if self.pending.is_none() {
            return true;
        }
        let Ok(permit) = self.sender.reserve().await else {
            return false;
        };
        let execution = self
            .pending
            .take()
            .expect("pending result owns the reserved slot");
        permit.send(context.update(UpdateData::RunFinished(execution)).into());
        true
    }
}

pub(in crate::monitor) async fn execute<M: From<ScenarioUpdate>>(
    spec: ScenarioSpec,
    context: ScenarioContext,
    budget: RunBudget,
    channels: ScenarioChannels<M>,
    stop: impl std::future::Future<Output = ()>,
) -> ScenarioExit {
    let mut reports = RunReports {
        sender: channels.reports,
        pending: None,
    };
    let execution = async {
        match spec {
            ScenarioSpec::SnmpGet(spec) => {
                super::impls::repeat_snmp_get::run(
                    spec,
                    context,
                    budget,
                    &mut reports,
                    channels.mode,
                )
                .await
            }
            ScenarioSpec::SnmpSet(spec) => {
                super::impls::repeat_snmp_set::run(
                    spec,
                    context,
                    budget,
                    &mut reports,
                    channels.mode,
                )
                .await
            }
        }
    };
    let exit = tokio::select! {
        biased;
        () = stop => ScenarioExit::Cancelled,
        outcome = AssertUnwindSafe(execution).catch_unwind() => outcome.unwrap_or_else(|panic|
            ScenarioExit::panicked(super::PanicStage::Runtime, panic_message(panic.as_ref()))),
    };
    reports.flush(context).await;
    exit
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::monitor) enum RunMode {
    Running,
    Paused,
}

/// Waits for the next scheduled operation, applying requested modes between operations.
pub(super) async fn wait_for_next_operation<M: From<ScenarioUpdate>>(
    timer: &mut PollTimer,
    mode_rx: &mut watch::Receiver<RunMode>,
    report_tx: &mpsc::Sender<M>,
    context: ScenarioContext,
) -> bool {
    let mut applied_mode = RunMode::Running;
    loop {
        let requested_mode = *mode_rx.borrow_and_update();
        if requested_mode != applied_mode {
            applied_mode = requested_mode;
            let data = match applied_mode {
                RunMode::Running => {
                    timer.resume(Instant::now());
                    UpdateData::Resumed
                }
                RunMode::Paused => UpdateData::Paused,
            };
            if report_tx.send(context.update(data).into()).await.is_err() {
                return false;
            }
            continue;
        }
        tokio::select! {
            biased;
            () = report_tx.closed() => return false,
            change = mode_rx.changed() => {
                if change.is_err() {
                    return false;
                }
            }
            () = timer.wait_next(), if applied_mode == RunMode::Running => return true,
        }
    }
}

/// Messages a scenario can send while its execution is still live.
pub(in crate::monitor) struct ScenarioUpdate {
    pub task_id: TaskId,
    pub generation: Generation,
    pub data: UpdateData,
}

pub(in crate::monitor) enum UpdateData {
    Progress(TaskProgress),
    Built,
    OperationStarted(crate::monitor::OperationStarted),
    RunFinished(ScenarioExecution),
    Paused,
    Resumed,
}

impl From<ScenarioUpdate> for ScenarioReport {
    fn from(update: ScenarioUpdate) -> Self {
        let ScenarioUpdate {
            task_id,
            generation,
            data,
        } = update;
        match data {
            UpdateData::Progress(progress) => Self::Progress {
                task_id,
                generation,
                progress,
            },
            UpdateData::Built => Self::Built {
                task_id,
                generation,
            },
            UpdateData::OperationStarted(operation) => Self::OperationStarted {
                task_id,
                context: operation.context,
                purpose: operation.purpose,
                started_at: operation.started_at,
                config: operation.config,
            },
            UpdateData::RunFinished(execution) => Self::RunFinished {
                task_id,
                generation,
                execution,
                limit_reached: false,
            },
            UpdateData::Paused => Self::Paused {
                task_id,
                generation,
            },
            UpdateData::Resumed => Self::Resumed {
                task_id,
                generation,
            },
        }
    }
}

#[derive(Debug)]
pub(in crate::monitor) enum ScenarioReport {
    Completed {
        task_id: TaskId,
        generation: Generation,
    },
    Progress {
        task_id: TaskId,
        generation: Generation,
        progress: TaskProgress,
    },
    OperationStarted {
        task_id: TaskId,
        context: OperationContext,
        purpose: String,
        started_at: DateTime<Utc>,
        config: AttemptConfig,
    },
    Built {
        task_id: TaskId,
        generation: Generation,
    },
    BuildFailed {
        task_id: TaskId,
        generation: Generation,
        error: ScenarioBuildError,
    },
    BuildPanicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    /// Marks a complete run and carries the report of its final operation.
    RunFinished {
        task_id: TaskId,
        generation: Generation,
        execution: ScenarioExecution,
        limit_reached: bool,
    },
    ExecutionFailed {
        task_id: TaskId,
        generation: Generation,
        error: ScenarioExecutionError,
        execution: ScenarioExecution,
    },
    ExecutionPanicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    Panicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    Paused {
        task_id: TaskId,
        generation: Generation,
    },
    Resumed {
        task_id: TaskId,
        generation: Generation,
    },
}

impl ScenarioReport {
    pub(in crate::monitor) const fn key(&self) -> (TaskId, Generation) {
        match self {
            Self::OperationStarted {
                task_id, context, ..
            } => (*task_id, context.generation),
            Self::Progress {
                task_id,
                generation,
                ..
            }
            | Self::Completed {
                task_id,
                generation,
            }
            | Self::Built {
                task_id,
                generation,
            }
            | Self::BuildFailed {
                task_id,
                generation,
                ..
            }
            | Self::BuildPanicked {
                task_id,
                generation,
                ..
            }
            | Self::RunFinished {
                task_id,
                generation,
                ..
            }
            | Self::ExecutionFailed {
                task_id,
                generation,
                ..
            }
            | Self::ExecutionPanicked {
                task_id,
                generation,
                ..
            }
            | Self::Panicked {
                task_id,
                generation,
                ..
            }
            | Self::Paused {
                task_id,
                generation,
            }
            | Self::Resumed {
                task_id,
                generation,
            } => (*task_id, *generation),
        }
    }
}

pub(super) struct Operations<M> {
    task_id: TaskId,
    generation: Generation,
    revision: SpecRevision,
    next_id: u64,
    report_tx: mpsc::Sender<M>,
}

impl<M: From<ScenarioUpdate>> Operations<M> {
    pub(super) fn new(
        task_id: TaskId,
        generation: Generation,
        revision: SpecRevision,
        report_tx: mpsc::Sender<M>,
    ) -> Self {
        Self {
            task_id,
            generation,
            revision,
            next_id: 0,
            report_tx,
        }
    }

    pub(super) async fn poll<A: Pollable>(
        &mut self,
        config: &AttemptConfig,
        adapter: &A,
        purpose: &str,
    ) -> Option<(OperationContext, PollReport<A::Output, A::Error>)> {
        let permit = self.report_tx.reserve().await.ok()?;
        let context =
            OperationContext::new(OperationId::new(self.next_id), self.generation, self.revision);
        self.next_id += 1;
        let mut start = Some((permit, purpose.to_string()));
        let report = poll_observed(config, adapter, |progress| {
            if let PollProgress::Started { at } = progress
                && let Some((permit, purpose)) = start.take()
            {
                permit.send(
                    ScenarioUpdate {
                        task_id: self.task_id,
                        generation: self.generation,
                        data: UpdateData::OperationStarted(crate::monitor::OperationStarted {
                            context,
                            purpose,
                            started_at: at,
                            config: *config,
                        }),
                    }
                    .into(),
                );
            }
        })
        .await;
        Some((context, report))
    }
}

pub(super) async fn connect(
    host: IpAddr,
    port: u16,
    community: Community,
    attempt: AttemptConfig,
) -> Result<SnmpClient, ScenarioBuildError> {
    let config = SnmpClientConfig {
        target: host,
        port,
        community,
        timeout: transport_timeout(&attempt),
        retries: 0,
        retry_delay: Duration::ZERO,
    };
    SnmpClient::new(config)
        .await
        .map_err(ScenarioBuildError::from)
}

pub(super) fn transport_timeout(attempt: &AttemptConfig) -> Duration {
    attempt.timeout().saturating_add(Duration::from_secs(1))
}

pub(super) fn build_timeout(attempt: &AttemptConfig) -> Duration {
    transport_timeout(attempt).saturating_mul(2)
}

pub(super) fn classify_error(error: SnmpError) -> AttemptError<SnmpError> {
    match error {
        SnmpError::Network { .. } | SnmpError::Timeout { .. } => AttemptError::Retryable(error),
        _ => AttemptError::NonRetryable(error),
    }
}

/// An error response from the device is a negative result of the request.
pub(super) fn fatal_request_error<T>(
    report: &PollReport<T, SnmpError>,
) -> Option<ScenarioExecutionError> {
    report
        .non_retryable_error()
        .filter(|error| !matches!(error, SnmpError::Protocol { .. }))
        .cloned()
        .map(ScenarioExecutionError::from)
}

pub(super) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use async_trait::async_trait;
    use tokio::time::sleep;

    use super::*;
    use crate::polling::{AttemptOutcome, PollOutcome};

    struct Step {
        elapsed: Duration,
        result: Result<u32, AttemptError<SnmpError>>,
    }

    struct Scripted(Mutex<VecDeque<Step>>);

    #[async_trait]
    impl Pollable for Scripted {
        type Output = u32;
        type Error = SnmpError;

        async fn poll(&self) -> Result<u32, AttemptError<SnmpError>> {
            let step = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected attempt");
            sleep(step.elapsed).await;
            step.result
        }
    }

    #[tokio::test(start_paused = true)]
    async fn operations_publish_one_start_for_all_attempts_with_exact_report_timing() {
        let generation = Generation::first().next();
        let revision = SpecRevision::new(7);
        let config =
            AttemptConfig::try_new(Duration::from_millis(100), 1, Duration::from_millis(10))
                .unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        let mut operations = Operations::new(TaskId(5), generation, revision, tx);
        let adapter = Scripted(Mutex::new(VecDeque::from([
            Step {
                elapsed: Duration::from_secs(1),
                result: Ok(1),
            },
            Step {
                elapsed: Duration::from_millis(5),
                result: Ok(2),
            },
            Step {
                elapsed: Duration::from_millis(1),
                result: Err(AttemptError::NonRetryable(SnmpError::InvalidOid(
                    "bad oid".to_string(),
                ))),
            },
        ])));
        let running = tokio::spawn(async move {
            let first = operations
                .poll(&config, &adapter, "Read state")
                .await
                .unwrap();
            let second = operations
                .poll(&config, &adapter, "Read state")
                .await
                .unwrap();
            (first, second)
        });
        let ScenarioReport::OperationStarted {
            task_id,
            context,
            purpose,
            started_at,
            config: actual,
        } = rx.recv().await.unwrap()
        else {
            panic!("expected operation start");
        };
        assert!(!running.is_finished());
        assert_eq!(task_id, TaskId(5));
        assert_eq!(context, OperationContext::new(OperationId::new(0), generation, revision));
        assert_eq!(purpose, "Read state");
        assert_eq!(actual, config);
        let ((first_context, first), (second_context, second)) = running.await.unwrap();
        assert_eq!(first_context, context);
        assert_eq!(first.started_at, started_at);
        assert_eq!(first.attempts.len(), 2);
        assert_eq!(first.attempts[0].outcome, AttemptOutcome::TimedOut);
        assert_eq!(first.outcome, PollOutcome::Succeeded(2));
        assert_eq!(first.elapsed, Duration::from_millis(115));
        let ScenarioReport::OperationStarted {
            context,
            started_at,
            ..
        } = rx.recv().await.unwrap()
        else {
            panic!("expected second operation start");
        };
        assert_eq!(context, second_context);
        assert_eq!(context, OperationContext::new(OperationId::new(1), generation, revision));
        assert_eq!(second.started_at, started_at);
        assert_eq!(second.outcome, PollOutcome::NonRetryableFailure);
        assert!(
            matches!(second.non_retryable_error(), Some(SnmpError::InvalidOid(value)) if value == "bad oid")
        );
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_attempts_remain_one_operation_and_new_runs_restart_numbering() {
        let config = AttemptConfig::try_new(Duration::from_millis(10), 1, Duration::ZERO).unwrap();
        for generation in [Generation::first(), Generation::first().next()] {
            let (tx, mut rx) = mpsc::channel(8);
            let mut operations = Operations::new(TaskId(5), generation, SpecRevision::new(1), tx);
            let adapter = Scripted(Mutex::new(VecDeque::from([
                Step {
                    elapsed: Duration::from_secs(1),
                    result: Ok(1),
                },
                Step {
                    elapsed: Duration::from_secs(1),
                    result: Ok(1),
                },
            ])));
            let (context, report) = operations
                .poll(&config, &adapter, "Write state")
                .await
                .unwrap();
            assert_eq!(context.operation_id, OperationId::new(0));
            assert_eq!(context.generation, generation);
            assert_eq!(report.outcome, PollOutcome::RetryLimitReached);
            assert_eq!(report.attempts.len(), 2);
            assert!(
                matches!(rx.try_recv().unwrap(), ScenarioReport::OperationStarted { context: actual, .. } if actual == context)
            );
            assert!(rx.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn closed_report_channel_prevents_starting_an_operation() {
        let (tx, rx) = mpsc::channel::<ScenarioReport>(1);
        drop(rx);
        let mut operations =
            Operations::new(TaskId(5), Generation::first(), SpecRevision::new(1), tx);
        let adapter = Scripted(Mutex::new(VecDeque::new()));
        let config = AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap();
        assert!(
            operations
                .poll(&config, &adapter, "Read state")
                .await
                .is_none()
        );
    }
}
