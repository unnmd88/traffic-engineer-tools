//! Сценарий «повторяющийся SNMP SET»: с заданным темпом отправляет устройству
//! SET-запросы.
//!
//! При сборке создаётся SNMP-соединение (при необходимости — отдельное
//! read-community для резолва SCN) и резолвятся SET-элементы. Затем по
//! расписанию выполняются SET-запросы; результат — успех или отсутствие ответа.

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpSetItem, oid::SnmpOid, value::SnmpValue},
};

use crate::monitor::OperationReport;
use crate::monitor::scenario::{
    ScenarioBuildError, ScenarioExecution, ScenarioOutcome, SnmpSetScenarioSpec, prepare,
    runtime::{
        Operations, RunBudget, RunMode, ScenarioContext, ScenarioReport, classify_error,
        fatal_request_error, panic_message, wait_for_next_operation,
    },
    schedule::PollTimer,
};

pub(in crate::monitor::scenario) async fn run(
    spec: SnmpSetScenarioSpec,
    context: ScenarioContext,
    mut budget: RunBudget,
    report_tx: mpsc::Sender<ScenarioReport>,
    mut mode_rx: watch::Receiver<RunMode>,
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
    while wait_for_next_operation(&mut timer, &mut mode_rx, &report_tx, context).await {
        let executed =
            AssertUnwindSafe(operations.poll(&attempt, &request, "Write configured SNMP values"))
                .catch_unwind()
                .await;
        match executed {
            Ok(None) => return,
            Ok(Some((context, report))) => {
                let error = fatal_request_error(&report);
                let execution = ScenarioExecution::new(
                    context,
                    OperationReport::from_poll(report, ScenarioOutcome::SnmpSet),
                );
                if let Some(error) = error {
                    let _ = report_tx
                        .send(ScenarioReport::ExecutionFailed {
                            task_id,
                            generation,
                            error,
                            execution,
                        })
                        .await;
                    return;
                }
                timer.execution_finished(Instant::now());
                let limit_reached = budget.record_finished_run();
                if report_tx
                    .send(ScenarioReport::RunFinished {
                        task_id,
                        generation,
                        execution,
                        limit_reached,
                    })
                    .await
                    .is_err()
                    || limit_reached
                {
                    return;
                }
            }
            Err(panic) => {
                let _ = report_tx
                    .send(ScenarioReport::ExecutionPanicked {
                        task_id,
                        generation,
                        reason: panic_message(panic.as_ref()),
                    })
                    .await;
                return;
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
