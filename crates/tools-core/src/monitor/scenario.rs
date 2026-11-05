use std::{net::IpAddr, panic::AssertUnwindSafe};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::FutureExt;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant, sleep_until, timeout},
};

use crate::{
    monitor::{
        probe::{
            ProbeBuildError, ProbeError, ProbeExecution, ProbeOutcome, ProbeSpecError, SnmpGetSpec,
            SnmpSetSpec, build_timeout,
        },
        runtime::worker::{Generation, WorkerCmd, WorkerReport},
        task::TaskId,
    },
    polling::{AttemptConfig, AttemptError, Pollable, poll},
    snmp::{
        SnmpClient, SnmpClientConfig, SnmpError, SnmpGetItem, SnmpGetSample, SnmpSetItem,
        business_value::BusinessValue, community::Community, oid::SnmpOid, resolve::Resolver,
        value::SnmpValue,
    },
};

#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Keep small scenario configurations inline"
)]
pub enum Scenario {
    SnmpGet(SnmpGetScenarioSpec),
    SnmpSet(SnmpSetScenarioSpec),
}

impl Scenario {
    #[expect(clippy::too_many_lines)]
    pub async fn run(
        self,
        task_id: TaskId,
        generation: Generation,
        report_tx: mpsc::Sender<WorkerReport>,
        mut cmd_rx: mpsc::Receiver<WorkerCmd>,
    ) {
        let panic_tx = report_tx.clone();
        let run = async move {
            match self {
                Self::SnmpGet(SnmpGetScenarioSpec {
                    request,
                    interval,
                    attempt,
                }) => {
                    let prepare = async {
                        let client =
                            connect(request.host, request.port, request.community, attempt).await?;
                        let items = Resolver::new(client.clone(), request.profile)
                            .resolve_get(request.oids)
                            .await
                            .map_err(|error| ProbeBuildError::Other(error.to_string()))?;
                        Ok::<_, ProbeBuildError>(SnmpGetRequest { client, items })
                    };
                    let built = AssertUnwindSafe(timeout(build_timeout(&attempt), prepare))
                        .catch_unwind()
                        .await;
                    let request = match built {
                        Ok(Ok(Ok(request))) => request,
                        Ok(Ok(Err(error))) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildFailed {
                                    task_id,
                                    generation,
                                    reason: error.to_string(),
                                })
                                .await;
                            return;
                        }
                        Ok(Err(_)) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildTimeout {
                                    task_id,
                                    generation,
                                })
                                .await;
                            return;
                        }
                        Err(panic) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildPanicked {
                                    task_id,
                                    generation,
                                    reason: panic_message(panic.as_ref()),
                                })
                                .await;
                            return;
                        }
                    };

                    let mut next_deadline = Instant::now() + interval;
                    if report_tx
                        .send(WorkerReport::Built {
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
                                Some(WorkerCmd::Resume) => {
                                    paused = false;
                                    next_deadline = Instant::now() + interval;
                                    if report_tx
                                        .send(WorkerReport::Resumed {
                                            task_id,
                                            generation,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Some(WorkerCmd::Pause) => {}
                                None => return,
                            }
                            continue;
                        }

                        tokio::select! {
                            biased;
                            () = report_tx.closed() => return,
                            cmd = cmd_rx.recv() => match cmd {
                                Some(WorkerCmd::Pause) => {
                                    paused = true;
                                    if report_tx.send(WorkerReport::Paused {
                                        task_id,
                                        generation,
                                    }).await.is_err() {
                                        return;
                                    }
                                }
                                Some(WorkerCmd::Resume) => {}
                                None => return,
                            },
                            () = sleep_until(next_deadline) => {
                                let started_at = Instant::now();
                                let executed = AssertUnwindSafe(poll(&attempt, &request))
                                    .catch_unwind()
                                    .await;
                                match executed {
                                    Ok(Ok(response)) => {
                                        let execution = ProbeExecution {
                                            finished_at: Utc::now(),
                                            elapsed: started_at.elapsed(),
                                            outcome: ProbeOutcome::SnmpGet(response),
                                        };
                                        next_deadline = Instant::now() + interval;
                                        if report_tx.send(WorkerReport::Executed {
                                            task_id,
                                            generation,
                                            execution,
                                        }).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(Err(error)) => {
                                        let _ = report_tx.send(WorkerReport::ExecutionFailed {
                                            task_id,
                                            generation,
                                            error: ProbeError::from(error),
                                        }).await;
                                        return;
                                    }
                                    Err(panic) => {
                                        let _ = report_tx.send(WorkerReport::ExecutionPanicked {
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
                Self::SnmpSet(SnmpSetScenarioSpec {
                    request,
                    interval,
                    attempt,
                }) => {
                    let prepare = async {
                        let client =
                            connect(request.host, request.port, request.community, attempt).await?;
                        let scn_client = match request.community_r {
                            Some(community) => {
                                connect(request.host, request.port, community, attempt).await?
                            }
                            None => client.clone(),
                        };
                        let items = Resolver::new(scn_client, request.profile)
                            .resolve_set(request.sets)
                            .await
                            .map_err(|error| ProbeBuildError::Other(error.to_string()))?;
                        Ok::<_, ProbeBuildError>(SnmpSetRequest { client, items })
                    };
                    let built = AssertUnwindSafe(timeout(build_timeout(&attempt), prepare))
                        .catch_unwind()
                        .await;
                    let request = match built {
                        Ok(Ok(Ok(request))) => request,
                        Ok(Ok(Err(error))) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildFailed {
                                    task_id,
                                    generation,
                                    reason: error.to_string(),
                                })
                                .await;
                            return;
                        }
                        Ok(Err(_)) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildTimeout {
                                    task_id,
                                    generation,
                                })
                                .await;
                            return;
                        }
                        Err(panic) => {
                            let _ = report_tx
                                .send(WorkerReport::BuildPanicked {
                                    task_id,
                                    generation,
                                    reason: panic_message(panic.as_ref()),
                                })
                                .await;
                            return;
                        }
                    };

                    let mut next_deadline = Instant::now() + interval;
                    if report_tx
                        .send(WorkerReport::Built {
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
                                Some(WorkerCmd::Resume) => {
                                    paused = false;
                                    next_deadline = Instant::now() + interval;
                                    if report_tx
                                        .send(WorkerReport::Resumed {
                                            task_id,
                                            generation,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Some(WorkerCmd::Pause) => {}
                                None => return,
                            }
                            continue;
                        }

                        tokio::select! {
                            biased;
                            () = report_tx.closed() => return,
                            cmd = cmd_rx.recv() => match cmd {
                                Some(WorkerCmd::Pause) => {
                                    paused = true;
                                    if report_tx.send(WorkerReport::Paused {
                                        task_id,
                                        generation,
                                    }).await.is_err() {
                                        return;
                                    }
                                }
                                Some(WorkerCmd::Resume) => {}
                                None => return,
                            },
                            () = sleep_until(next_deadline) => {
                                let started_at = Instant::now();
                                let executed = AssertUnwindSafe(poll(&attempt, &request))
                                    .catch_unwind()
                                    .await;
                                match executed {
                                    Ok(Ok(response)) => {
                                        let execution = ProbeExecution {
                                            finished_at: Utc::now(),
                                            elapsed: started_at.elapsed(),
                                            outcome: ProbeOutcome::SnmpSet(response),
                                        };
                                        next_deadline = Instant::now() + interval;
                                        if report_tx.send(WorkerReport::Executed {
                                            task_id,
                                            generation,
                                            execution,
                                        }).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(Err(error)) => {
                                        let _ = report_tx.send(WorkerReport::ExecutionFailed {
                                            task_id,
                                            generation,
                                            error: ProbeError::from(error),
                                        }).await;
                                        return;
                                    }
                                    Err(panic) => {
                                        let _ = report_tx.send(WorkerReport::ExecutionPanicked {
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
            }
        };
        if let Err(panic) = AssertUnwindSafe(run).catch_unwind().await {
            let _ = panic_tx
                .send(WorkerReport::WorkerPanicked {
                    task_id,
                    generation,
                    reason: panic_message(panic.as_ref()),
                })
                .await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnmpGetScenarioSpec {
    request: SnmpGetSpec,
    interval: Duration,
    attempt: AttemptConfig,
}

impl SnmpGetScenarioSpec {
    pub fn try_new(
        request: SnmpGetSpec,
        interval: Duration,
        attempt: AttemptConfig,
    ) -> Result<Self, ProbeSpecError> {
        validate_interval(interval, attempt)?;
        Ok(Self {
            request,
            interval,
            attempt,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SnmpSetScenarioSpec {
    request: SnmpSetSpec,
    interval: Duration,
    attempt: AttemptConfig,
}

impl SnmpSetScenarioSpec {
    pub fn try_new(
        request: SnmpSetSpec,
        interval: Duration,
        attempt: AttemptConfig,
    ) -> Result<Self, ProbeSpecError> {
        validate_interval(interval, attempt)?;
        Ok(Self {
            request,
            interval,
            attempt,
        })
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

fn validate_interval(interval: Duration, attempt: AttemptConfig) -> Result<(), ProbeSpecError> {
    if interval.is_zero() {
        return Err(ProbeSpecError::IntervalMustBePositive);
    }
    let budget = attempt.budget();
    if interval < budget {
        return Err(ProbeSpecError::IntervalShorterThanAttemptBudget {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            budget_ms: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

async fn connect(
    host: IpAddr,
    port: u16,
    community: Community,
    attempt: AttemptConfig,
) -> Result<SnmpClient, ProbeBuildError> {
    let config = SnmpClientConfig {
        target: host,
        port,
        community,
        timeout: attempt.timeout().saturating_add(Duration::from_secs(1)),
        retries: 0,
        retry_delay: Duration::ZERO,
    };
    SnmpClient::new(config)
        .await
        .map_err(|_| ProbeBuildError::SnmpClientCreate)
}

fn classify_error(error: &SnmpError) -> AttemptError {
    match error {
        SnmpError::Network { .. } | SnmpError::Timeout { .. } => {
            AttemptError::Transient(error.to_string())
        }
        _ => AttemptError::Fatal(error.to_string()),
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

#[cfg(test)]
mod tests;
