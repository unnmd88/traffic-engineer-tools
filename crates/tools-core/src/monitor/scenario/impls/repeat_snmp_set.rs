//! Сценарий «повторяющийся SNMP SET»: с заданным темпом отправляет устройству
//! SET-запросы.
//!
//! При сборке создаётся SNMP-соединение (при необходимости — отдельное
//! read-community для резолва SCN) и резолвятся SET-элементы. Затем по
//! расписанию выполняются SET-запросы; результат — успех или отсутствие ответа.

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::{sync::watch, time::Instant};

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpSetItem, oid::SnmpOid, value::SnmpValue},
};

use crate::monitor::OperationReport;
use crate::monitor::scenario::{
    PanicStage, ScenarioExecution, ScenarioExit, ScenarioOutcome, SnmpSetScenarioSpec, prepare,
    runtime::{
        Operations, RunBudget, RunMode, RunReports, ScenarioContext, ScenarioUpdate, UpdateData,
        classify_error, fatal_request_error, panic_message, wait_for_next_operation,
    },
    schedule::PollTimer,
};

pub(in crate::monitor::scenario) async fn run<M: From<ScenarioUpdate>>(
    spec: SnmpSetScenarioSpec,
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
    let SnmpSetScenarioSpec {
        request,
        schedule,
        attempt,
    } = spec;
    let request =
        match prepare::set(request, attempt, task_id, generation, reports.sender.clone()).await {
            Ok((client, items)) => SnmpSetRequest { client, items },
            Err(error) => return ScenarioExit::build_failed(error),
        };

    let mut timer = PollTimer::new(schedule, Instant::now());
    if reports
        .sender
        .send(context.update(UpdateData::Built))
        .await
        .is_err()
    {
        return ScenarioExit::Cancelled;
    }

    let mut operations = Operations::new(task_id, generation, revision, reports.sender.clone());
    while wait_for_next_operation(&mut timer, &mut mode_rx, &reports.sender, context).await {
        let polling = operations.poll(&attempt, &request, "Write configured SNMP values");
        let executed = AssertUnwindSafe(polling).catch_unwind().await;
        match executed {
            Ok(None) => return ScenarioExit::Cancelled,
            Ok(Some((operation, report))) => {
                let error = fatal_request_error(&report);
                let execution = ScenarioExecution::new(
                    operation,
                    OperationReport::from_poll(report, ScenarioOutcome::SnmpSet),
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
