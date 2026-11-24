//! Сценарий «повторяющийся SNMP GET»: с заданным темпом опрашивает устройство
//! GET-запросами и возвращает распарсенные значения.
//!
//! При сборке создаётся SNMP-соединение и резолвятся OID (по профилю, если он
//! задан). Затем по расписанию выполняются GET-запросы; каждый результат —
//! список `SnmpGetSample` (имя, OID, сырое и бизнес-значение).

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpGetItem, SnmpGetSample, business_value::BusinessValue, oid::SnmpOid},
};

use crate::monitor::OperationReport;
use crate::monitor::scenario::{
    ScenarioBuildError, ScenarioExecution, ScenarioOutcome, SnmpGetScenarioSpec, prepare,
    runtime::{
        Operations, RunBudget, RunMode, ScenarioContext, ScenarioReport, classify_error,
        fatal_request_error, panic_message, wait_for_next_operation,
    },
    schedule::PollTimer,
};

pub(in crate::monitor::scenario) async fn run(
    spec: SnmpGetScenarioSpec,
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
    let SnmpGetScenarioSpec {
        request,
        schedule,
        attempt,
    } = spec;
    let request = match prepare::get(request, attempt, task_id, generation, report_tx.clone()).await
    {
        Ok((client, items)) => SnmpGetRequest { client, items },
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
            AssertUnwindSafe(operations.poll(&attempt, &request, "Read configured SNMP OIDs"))
                .catch_unwind()
                .await;
        match executed {
            Ok(None) => return,
            Ok(Some((context, report))) => {
                let error = fatal_request_error(&report);
                let execution = ScenarioExecution::new(
                    context,
                    OperationReport::from_poll(report, ScenarioOutcome::SnmpGet),
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

struct SnmpGetRequest {
    client: SnmpClient,
    items: Vec<SnmpGetItem>,
}

#[async_trait]
impl Pollable for SnmpGetRequest {
    type Error = crate::snmp::SnmpError;
    type Output = Vec<SnmpGetSample>;

    async fn poll(&self) -> Result<Self::Output, AttemptError<Self::Error>> {
        let oids: Vec<SnmpOid> = self.items.iter().map(|item| item.oid.clone()).collect();
        let varbinds = self.client.get_many(&oids).await.map_err(classify_error)?;

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
