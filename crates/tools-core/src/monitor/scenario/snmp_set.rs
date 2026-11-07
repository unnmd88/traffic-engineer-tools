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
    snmp::{SnmpClient, SnmpSetItem, oid::SnmpOid, resolve::Resolver, value::SnmpValue},
};

use super::{
    Generation, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError, ScenarioOutcome,
    SnmpSetScenarioSpec,
    runtime::{
        ScenarioCommand, ScenarioReport, build_timeout, classify_error, connect, panic_message,
    },
    schedule::PollTimer,
};

#[expect(clippy::too_many_lines)]
pub(super) async fn run(
    spec: SnmpSetScenarioSpec,
    task_id: TaskId,
    generation: Generation,
    report_tx: mpsc::Sender<ScenarioReport>,
    mut cmd_rx: mpsc::Receiver<ScenarioCommand>,
) {
    let SnmpSetScenarioSpec {
        request,
        schedule,
        attempt,
    } = spec;
    let prepare = async {
        let (host, port, community_r, community, profile, sets) = request.into_parts();
        let client = connect(host, port, community, attempt).await?;
        let scn_client = match community_r {
            Some(community) => connect(host, port, community, attempt).await?,
            None => client.clone(),
        };
        let items = Resolver::new(scn_client, profile)
            .resolve_set(sets)
            .await
            .map_err(|error| ScenarioBuildError::Other(error.to_string()))?;
        Ok::<_, ScenarioBuildError>(SnmpSetRequest { client, items })
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
                            outcome: ScenarioOutcome::SnmpSet(response),
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

struct SnmpSetRequest {
    client: SnmpClient,
    items: Vec<SnmpSetItem>,
}

#[async_trait]
impl Pollable for SnmpSetRequest {
    type Output = ();

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let sets: Vec<(SnmpOid, SnmpValue)> = self
            .items
            .iter()
            .map(|item| (item.oid.clone(), item.value.clone()))
            .collect();
        self.client
            .set_many(&sets)
            .await
            .map_err(|error| classify_error(&error))?;
        Ok(())
    }
}
