use std::{net::IpAddr, panic::AssertUnwindSafe};

use futures_util::FutureExt;
use tokio::{sync::mpsc, time::Duration};

use crate::{
    monitor::task::TaskId,
    polling::{AttemptConfig, AttemptError},
    snmp::{SnmpClient, SnmpClientConfig, SnmpError, community::Community},
};

use super::{
    Generation, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError, ScenarioSpec,
};

pub(in crate::monitor) async fn run(
    spec: ScenarioSpec,
    task_id: TaskId,
    generation: Generation,
    report_tx: mpsc::Sender<ScenarioReport>,
    cmd_rx: mpsc::Receiver<ScenarioCommand>,
) {
    let panic_tx = report_tx.clone();
    let execution = async move {
        match spec {
            ScenarioSpec::SnmpGet(spec) => {
                super::snmp_get::run(spec, task_id, generation, report_tx, cmd_rx).await;
            }
            ScenarioSpec::SnmpSet(spec) => {
                super::snmp_set::run(spec, task_id, generation, report_tx, cmd_rx).await;
            }
        }
    };
    if let Err(panic) = AssertUnwindSafe(execution).catch_unwind().await {
        let _ = panic_tx
            .send(ScenarioReport::Panicked {
                task_id,
                generation,
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
            Self::Built {
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
        .map_err(|_| ScenarioBuildError::SnmpClientCreate)
}

fn transport_timeout(attempt: &AttemptConfig) -> Duration {
    attempt.timeout().saturating_add(Duration::from_secs(1))
}

pub(super) fn build_timeout(attempt: &AttemptConfig) -> Duration {
    transport_timeout(attempt).saturating_mul(2)
}

pub(super) fn classify_error(error: &SnmpError) -> AttemptError {
    match error {
        SnmpError::Network { .. } | SnmpError::Timeout { .. } => {
            AttemptError::Transient(error.to_string())
        }
        _ => AttemptError::Fatal(error.to_string()),
    }
}

pub(super) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
