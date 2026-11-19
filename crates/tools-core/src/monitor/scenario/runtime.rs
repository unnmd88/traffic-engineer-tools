use std::{net::IpAddr, panic::AssertUnwindSafe};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::{sync::mpsc, time::Duration};

use crate::{
    monitor::{OperationContext, OperationId, SpecRevision, task::TaskId},
    polling::{AttemptConfig, AttemptError, PollProgress, PollReport, Pollable, poll_observed},
    snmp::{SnmpClient, SnmpClientConfig, SnmpError, community::Community},
};

use super::{
    Generation, PreparationEvent, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError,
    ScenarioSpec,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::monitor) struct ScenarioContext {
    pub task_id: TaskId,
    pub generation: Generation,
    pub revision: SpecRevision,
}

pub(in crate::monitor) async fn run(
    spec: ScenarioSpec,
    context: ScenarioContext,
    report_tx: mpsc::Sender<ScenarioReport>,
    cmd_rx: mpsc::Receiver<ScenarioCommand>,
) {
    let panic_tx = report_tx.clone();
    let execution = async move {
        match spec {
            ScenarioSpec::SnmpGet(spec) => {
                super::impls::repeat_snmp_get::run(spec, context, report_tx, cmd_rx).await;
            }
            ScenarioSpec::SnmpSet(spec) => {
                super::impls::repeat_snmp_set::run(spec, context, report_tx, cmd_rx).await;
            }
        }
    };
    if let Err(panic) = AssertUnwindSafe(execution).catch_unwind().await {
        let _ = panic_tx
            .send(ScenarioReport::Panicked {
                task_id: context.task_id,
                generation: context.generation,
                reason: panic_message(panic.as_ref()),
            })
            .await;
    }
}

#[derive(Debug)]
pub(in crate::monitor) enum ScenarioCommand {
    Pause,
    Resume,
}

#[derive(Debug)]
pub(in crate::monitor) enum ScenarioReport {
    OperationStarted {
        task_id: TaskId,
        context: OperationContext,
        purpose: String,
        started_at: DateTime<Utc>,
        config: AttemptConfig,
    },
    Preparation {
        task_id: TaskId,
        generation: Generation,
        event: Box<PreparationEvent>,
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
    Executed {
        task_id: TaskId,
        generation: Generation,
        execution: ScenarioExecution,
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
            Self::Preparation {
                task_id,
                generation,
                ..
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
            | Self::Executed {
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

pub(super) struct Operations {
    task_id: TaskId,
    generation: Generation,
    revision: SpecRevision,
    next_id: u64,
    report_tx: mpsc::Sender<ScenarioReport>,
}

impl Operations {
    pub(super) fn new(
        task_id: TaskId,
        generation: Generation,
        revision: SpecRevision,
        report_tx: mpsc::Sender<ScenarioReport>,
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
                permit.send(ScenarioReport::OperationStarted {
                    task_id: self.task_id,
                    context,
                    purpose,
                    started_at: at,
                    config: *config,
                });
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
        let (tx, rx) = mpsc::channel(1);
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
