//! Сценарий «повторяющийся SNMP SET»: с заданным темпом отправляет устройству
//! SET-запросы.
//!
//! При сборке создаётся SNMP-соединение (при необходимости — отдельное
//! read-community для резолва SCN) и резолвятся SET-элементы. Затем по
//! расписанию выполняются SET-запросы; результат — успех или отсутствие ответа.

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::{sync::mpsc, time::Instant};

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpSetItem, oid::SnmpOid, value::SnmpValue},
};

use crate::monitor::scenario::{
    ScenarioBuildError, ScenarioExecution, ScenarioExecutionError, ScenarioOutcome,
    SnmpSetScenarioSpec, prepare,
    runtime::{
        Operations, ScenarioCommand, ScenarioContext, ScenarioReport, classify_error, panic_message,
    },
    schedule::PollTimer,
};

#[expect(clippy::too_many_lines)]
pub(in crate::monitor::scenario) async fn run(
    spec: SnmpSetScenarioSpec,
    context: ScenarioContext,
    report_tx: mpsc::Sender<ScenarioReport>,
    mut cmd_rx: mpsc::Receiver<ScenarioCommand>,
) {
    let ScenarioContext {
        task_id,
        generation,
        revision,
    } = context;
    let SnmpSetScenarioSpec {
        request,
        schedule,
        attempt,
    } = spec;
    let request = match prepare::set(request, attempt, task_id, generation, report_tx.clone()).await
    {
        Ok((client, items)) => SnmpSetRequest { client, items },
        Err(ScenarioBuildError::Panicked(reason)) => {
            let _ = report_tx
                .send(ScenarioReport::BuildPanicked {
                    task_id,
                    generation,
                    reason,
                })
                .await;
            return;
        }
        Err(error) => {
            let _ = report_tx
                .send(ScenarioReport::BuildFailed {
                    task_id,
                    generation,
                    error,
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

    let mut operations = Operations::new(task_id, generation, revision, report_tx.clone());
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
                let executed = AssertUnwindSafe(operations.poll(&attempt, &request, "Write configured SNMP values"))
                    .catch_unwind()
                    .await;
                match executed {
                    Ok(None) => return,
                    Ok(Some((context, report))) => {
                        let error = report.non_retryable_error().cloned();
                        let execution = ScenarioExecution::new(context, ScenarioOutcome::SnmpSet(report));
                        if let Some(error) = error {
                            let _ = report_tx.send(ScenarioReport::ExecutionFailed {
                                task_id,
                                generation,
                                error: ScenarioExecutionError::from(error),
                                execution,
                            }).await;
                            return;
                        }
                        timer.execution_finished(Instant::now());
                        if report_tx.send(ScenarioReport::Executed {
                            task_id,
                            generation,
                            execution,
                        }).await.is_err() {
                            return;
                        }
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
    type Error = crate::snmp::SnmpError;
    type Output = ();

    async fn poll(&self) -> Result<Self::Output, AttemptError<Self::Error>> {
        let sets: Vec<(SnmpOid, SnmpValue)> = self
            .items
            .iter()
            .map(|item| (item.oid.clone(), item.value.clone()))
            .collect();
        self.client.set_many(&sets).await.map_err(classify_error)?;
        Ok(())
    }
}
