//! Сценарий «повторяющийся SNMP GET»: с заданным темпом опрашивает устройство
//! GET-запросами и возвращает распарсенные значения.
//!
//! При сборке создаётся SNMP-соединение и резолвятся OID (по профилю, если он
//! задан). Затем по расписанию выполняются GET-запросы; каждый результат —
//! список `SnmpGetSample` (имя, OID, сырое и бизнес-значение).

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::{sync::watch, time::Instant};

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpGetItem, SnmpGetSample, business_value::BusinessValue, oid::SnmpOid},
};

use crate::monitor::OperationReport;
use crate::monitor::scenario::{
    PanicStage, ScenarioExecution, ScenarioExit, ScenarioOutcome, SnmpGetScenarioSpec, prepare,
    runtime::{
        Operations, RunBudget, RunMode, RunReports, ScenarioContext, ScenarioUpdate, UpdateData,
        classify_error, fatal_request_error, panic_message, wait_for_next_operation,
    },
    schedule::PollTimer,
};

pub(in crate::monitor::scenario) async fn run<M: From<ScenarioUpdate>>(
    spec: SnmpGetScenarioSpec,
    context: ScenarioContext,
    mut budget: RunBudget,
    reports: &mut RunReports<M>,
    mut mode_rx: watch::Receiver<RunMode>,
) -> ScenarioExit {
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
    let request =
        match prepare::get(request, attempt, task_id, generation, reports.sender.clone()).await {
            Ok((client, items)) => SnmpGetRequest { client, items },
            Err(error) => return ScenarioExit::build_failed(error),
        };

    let mut timer = PollTimer::new(schedule, Instant::now());
    if reports
        .sender
        .send(context.update(UpdateData::Built).into())
        .await
        .is_err()
    {
        return ScenarioExit::Cancelled;
    }

    let mut operations = Operations::new(task_id, generation, revision, reports.sender.clone());
    while wait_for_next_operation(&mut timer, &mut mode_rx, &reports.sender, context).await {
        let polling = operations.poll(&attempt, &request, "Read configured SNMP OIDs");
        let executed = AssertUnwindSafe(polling).catch_unwind().await;
        match executed {
            Ok(None) => return ScenarioExit::Cancelled,
            Ok(Some((operation, report))) => {
                let error = fatal_request_error(&report);
                let execution = ScenarioExecution::new(
                    operation,
                    OperationReport::from_poll(report, ScenarioOutcome::SnmpGet),
                );
                if let Some(error) = error {
                    return ScenarioExit::ExecutionFailed(error, Box::new(execution));
                }
                timer.execution_finished(Instant::now());
                if budget.record_finished_run() {
                    return ScenarioExit::Completed(Some(execution));
                }
                if !reports.finished(context, execution).await {
                    return ScenarioExit::Cancelled;
                }
            }
            Err(panic) => {
                return ScenarioExit::panicked(
                    PanicStage::Execution,
                    panic_message(panic.as_ref()),
                );
            }
        }
    }
    ScenarioExit::Cancelled
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
