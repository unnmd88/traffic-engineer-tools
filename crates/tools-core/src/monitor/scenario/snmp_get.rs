use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::FutureExt;
use tokio::{
    sync::mpsc,
    time::{Instant, timeout},
};

use crate::{
    monitor::task::TaskId,
    polling::{AttemptError, Pollable, poll},
    snmp::{
        SnmpClient, SnmpGetItem, SnmpGetSample, business_value::BusinessValue, oid::SnmpOid,
        resolve::Resolver,
    },
};

use super::{
    Generation, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError, ScenarioOutcome,
    SnmpGetScenarioSpec,
    runtime::{
        ScenarioCommand, ScenarioReport, build_timeout, classify_error, connect, panic_message,
    },
    schedule::PollTimer,
};

#[expect(clippy::too_many_lines)]
pub(super) async fn run(
    spec: SnmpGetScenarioSpec,
    task_id: TaskId,
    generation: Generation,
    report_tx: mpsc::Sender<ScenarioReport>,
    mut cmd_rx: mpsc::Receiver<ScenarioCommand>,
) {
    let SnmpGetScenarioSpec {
        request,
        schedule,
        attempt,
    } = spec;
    let prepare = async {
        let client = connect(request.host, request.port, request.community, attempt).await?;
        let items = Resolver::new(client.clone(), request.profile)
            .resolve_get(request.oids)
            .await
            .map_err(|error| ScenarioBuildError::Other(error.to_string()))?;
        Ok::<_, ScenarioBuildError>(SnmpGetRequest { client, items })
    };
    let built = AssertUnwindSafe(timeout(build_timeout(&attempt), prepare))
        .catch_unwind()
        .await;
    let request = match built {
        Ok(Ok(Ok(request))) => request,
        Ok(Ok(Err(error))) => {
            let _ = report_tx
                .send(ScenarioReport::BuildFailed {
                    task_id,
                    generation,
                    error,
                })
                .await;
            return;
        }
        Ok(Err(_)) => {
            let _ = report_tx
                .send(ScenarioReport::BuildFailed {
                    task_id,
                    generation,
                    error: ScenarioBuildError::Timeout,
                })
                .await;
            return;
        }
        Err(panic) => {
            let _ = report_tx
                .send(ScenarioReport::BuildPanicked {
                    task_id,
                    generation,
                    reason: panic_message(panic.as_ref()),
                })
                .await;
            return;
        }
    };

    let mut timer = PollTimer::new(schedule, Instant::now());
    if report_tx
        .send(ScenarioReport::Built {
            task_id,
            generation,
        })
        .await
        .is_err()
    {
        return;
    }

    let mut paused = false;
    loop {
        if paused {
            let cmd = tokio::select! {
                () = report_tx.closed() => return,
                cmd = cmd_rx.recv() => cmd,
            };
            match cmd {
                Some(ScenarioCommand::Resume) => {
                    paused = false;
                    timer.resume(Instant::now());
                    if report_tx
                        .send(ScenarioReport::Resumed {
                            task_id,
                            generation,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(ScenarioCommand::Pause) => {}
                None => return,
            }
            continue;
        }

        tokio::select! {
            biased;
            () = report_tx.closed() => return,
            cmd = cmd_rx.recv() => match cmd {
                Some(ScenarioCommand::Pause) => {
                    paused = true;
                    if report_tx.send(ScenarioReport::Paused {
                        task_id,
                        generation,
                    }).await.is_err() {
                        return;
                    }
                }
                Some(ScenarioCommand::Resume) => {}
                None => return,
            },
            () = timer.wait_next() => {
                let started_at = Instant::now();
                let executed = AssertUnwindSafe(poll(&attempt, &request))
                    .catch_unwind()
                    .await;
                match executed {
                    Ok(Ok(response)) => {
                        let execution = ScenarioExecution {
                            finished_at: Utc::now(),
                            elapsed: started_at.elapsed(),
                            outcome: ScenarioOutcome::SnmpGet(response),
                        };
                        timer.execution_finished(Instant::now());
                        if report_tx.send(ScenarioReport::Executed {
                            task_id,
                            generation,
                            execution,
                        }).await.is_err() {
                            return;
                        }
                    }
                    Ok(Err(error)) => {
                        let _ = report_tx.send(ScenarioReport::ExecutionFailed {
                            task_id,
                            generation,
                            error: ScenarioExecutionError::from(error),
                        }).await;
                        return;
                    }
                    Err(panic) => {
                        let _ = report_tx.send(ScenarioReport::ExecutionPanicked {
                            task_id,
                            generation,
                            reason: panic_message(panic.as_ref()),
                        }).await;
                        return;
                    }
                }
            }
        }
    }
}

struct SnmpGetRequest {
    client: SnmpClient,
    items: Vec<SnmpGetItem>,
}

#[async_trait]
impl Pollable for SnmpGetRequest {
    type Output = Vec<SnmpGetSample>;

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let oids: Vec<SnmpOid> = self.items.iter().map(|item| item.oid.clone()).collect();
        let varbinds = self
            .client
            .get_many(&oids)
            .await
            .map_err(|error| classify_error(&error))?;

        Ok(varbinds
            .into_iter()
            .zip(&self.items)
            .map(|(varbind, item)| {
                let value = item.parser.map(|parser| {
                    parser(&varbind.value).unwrap_or_else(|error| {
                        tracing::error!(target: "snmp_reader", value = ?varbind.value, "{error}");
                        BusinessValue::Text("parse error".to_string())
                    })
                });
                SnmpGetSample {
                    oid_name: item.name.clone(),
                    oid: varbind.oid,
                    raw_value: varbind.value,
                    value,
                }
            })
            .collect())
    }
}
