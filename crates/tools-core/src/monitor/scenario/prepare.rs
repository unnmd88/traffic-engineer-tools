use std::{future::Future, net::SocketAddr, panic::AssertUnwindSafe};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::FutureExt;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant, timeout_at},
};
use tracing::Instrument;

use crate::{
    Scn,
    monitor::{TaskProgress, task::TaskId},
    polling::{
        AttemptConfig, AttemptError, AttemptOutcome, PollOutcome, PollProgress, PollReport,
        Pollable, poll_observed,
    },
    snmp::{
        Community, SnmpClient, SnmpError, SnmpGetItem, SnmpGetSpec, SnmpOid, SnmpSetItem,
        SnmpSetSpec, SnmpVarbind, resolve::Resolver, site_id::fetch_site_id_potok_ug405,
    },
};

use super::{
    Generation, ScenarioBuildError,
    runtime::{
        ScenarioUpdate, UpdateData, build_timeout, classify_error, connect, panic_message,
        transport_timeout,
    },
};

type ScnRequestReport = PollReport<SnmpVarbind, SnmpError>;

struct Preparation<M> {
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<M>,
    deadline: Instant,
}

impl<M: From<ScenarioUpdate>> Preparation<M> {
    fn new(
        task_id: TaskId,
        generation: Generation,
        reports: mpsc::Sender<M>,
        attempt: AttemptConfig,
    ) -> Self {
        Self {
            task_id,
            generation,
            reports,
            deadline: Instant::now() + build_timeout(&attempt),
        }
    }

    async fn progress(&self, message: &str) {
        let progress = TaskProgress {
            at: Utc::now(),
            message: message.to_string(),
        };
        tracing::info!(task_id = %self.task_id, generation = ?self.generation,
            at = %progress.at, message = %progress.message, "scenario preparation progress");
        let _ = self
            .reports
            .send(
                ScenarioUpdate {
                    task_id: self.task_id,
                    generation: self.generation,
                    data: UpdateData::Progress(progress),
                }
                .into(),
            )
            .await;
    }

    async fn action<T>(
        &self,
        message: &str,
        execute: impl Future<Output = Result<T, ScenarioBuildError>>,
    ) -> Result<T, ScenarioBuildError> {
        self.progress(message).await;
        let started = Instant::now();
        let executed = AssertUnwindSafe(timeout_at(self.deadline, execute)
            .instrument(tracing::info_span!("preparation_action", task_id = %self.task_id, generation = ?self.generation, message)))
            .catch_unwind().await;
        let result = match executed {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ScenarioBuildError::Timeout),
            Err(panic) => Err(ScenarioBuildError::Panicked(panic_message(panic.as_ref()))),
        };
        match &result {
            Ok(_) => {
                tracing::info!(task_id = %self.task_id, generation = ?self.generation, message,
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0, "preparation action finished")
            }
            Err(error) => {
                tracing::warn!(task_id = %self.task_id, generation = ?self.generation, message,
                %error, elapsed_ms = started.elapsed().as_secs_f64() * 1000.0, "preparation action failed")
            }
        }
        result
    }

    async fn transport(
        &self,
        target: SocketAddr,
        community: Community,
        message: &str,
        attempt: AttemptConfig,
    ) -> Result<SnmpClient, ScenarioBuildError> {
        self.action(message, connect(target.ip(), target.port(), community, attempt))
            .await
    }

    async fn scn(
        &self,
        resolver: &Resolver,
        client: &SnmpClient,
        oids: &[SnmpOid],
        attempt: AttemptConfig,
    ) -> Result<Option<Scn>, ScenarioBuildError> {
        if !resolver.needs_scn(oids) {
            return Ok(None);
        }
        let settings = AttemptConfig::try_new(transport_timeout(&attempt), 0, Duration::ZERO)
            .expect("transport timeout is positive");
        self.action("Reading SCN: selected OIDs require a site identifier", async {
            let request = ScnRequest {
                client: client.clone(),
            };
            let report = poll_observed(&settings, &request, log_attempt).await;
            decode_scn(&report)
        })
        .await
        .map(Some)
    }
}

fn log_attempt(progress: PollProgress<'_, SnmpError>) {
    match progress {
        PollProgress::Started { .. } => {}
        PollProgress::AttemptStarted {
            number,
            started_after,
        } => tracing::info!(
            attempt = number,
            started_after_ms = started_after.as_secs_f64() * 1000.0,
            "SCN request attempt started"
        ),
        PollProgress::AttemptFinished { number, report } => match &report.outcome {
            AttemptOutcome::Succeeded => tracing::info!(
                attempt = number,
                elapsed_ms = report.elapsed.as_secs_f64() * 1000.0,
                "SCN request attempt succeeded"
            ),
            outcome => tracing::warn!(
                attempt = number,
                ?outcome,
                elapsed_ms = report.elapsed.as_secs_f64() * 1000.0,
                "SCN request attempt failed"
            ),
        },
    }
}

struct ScnRequest {
    client: SnmpClient,
}

#[async_trait]
impl Pollable for ScnRequest {
    type Output = SnmpVarbind;
    type Error = SnmpError;

    async fn poll(&self) -> Result<Self::Output, AttemptError<Self::Error>> {
        fetch_site_id_potok_ug405(&self.client)
            .await
            .map_err(classify_error)
    }
}

fn decode_scn(report: &ScnRequestReport) -> Result<Scn, ScenarioBuildError> {
    match &report.outcome {
        PollOutcome::Succeeded(varbind) => {
            let bytes = varbind
                .value
                .as_bytes()
                .ok_or_else(|| SnmpError::UnexpectedValueType {
                    expected: "OctetString".to_string(),
                    actual: varbind.value.as_string(),
                })?;
            Scn::from_bytes(bytes).map_err(|error| SnmpError::ConvertScn(error.to_string()).into())
        }
        PollOutcome::RetryLimitReached | PollOutcome::NonRetryableFailure => {
            let error = match report.attempts.last().map(|attempt| &attempt.outcome) {
                Some(AttemptOutcome::Failed { error, .. }) => {
                    ScenarioBuildError::Snmp(error.clone())
                }
                _ => ScenarioBuildError::Timeout,
            };
            Err(error)
        }
    }
}

pub(super) async fn get<M: From<ScenarioUpdate>>(
    spec: SnmpGetSpec,
    attempt: AttemptConfig,
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<M>,
) -> Result<(SnmpClient, Vec<SnmpGetItem>), ScenarioBuildError> {
    let preparation = Preparation::new(task_id, generation, reports, attempt);
    async {
        let (host, port, community, profile, items) = spec.into_parts();
        let client = preparation
            .transport(SocketAddr::new(host, port), community, "Creating SNMP read client", attempt)
            .await?;
        let resolver = Resolver::new(profile);
        let oids: Vec<_> = items.iter().map(|item| item.oid.clone()).collect();
        let scn = preparation.scn(&resolver, &client, &oids, attempt).await?;
        let items = preparation
            .action("Resolving OIDs", async {
                resolver
                    .resolve_get(items, scn.as_ref())
                    .map_err(ScenarioBuildError::from)
            })
            .await?;
        Ok((client, items))
    }
    .await
}

pub(super) async fn set<M: From<ScenarioUpdate>>(
    spec: SnmpSetSpec,
    attempt: AttemptConfig,
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<M>,
) -> Result<(SnmpClient, Vec<SnmpSetItem>), ScenarioBuildError> {
    let preparation = Preparation::new(task_id, generation, reports, attempt);
    async {
        let (host, port, community_r, community, profile, items) = spec.into_parts();
        let target = SocketAddr::new(host, port);
        let client = preparation
            .transport(target, community, "Creating SNMP write client", attempt)
            .await?;
        let scn_client = match community_r {
            Some(community) => {
                preparation
                    .transport(target, community, "Creating SNMP read client", attempt)
                    .await?
            }
            None => client.clone(),
        };
        let resolver = Resolver::new(profile);
        let oids: Vec<_> = items.iter().map(|item| item.oid.clone()).collect();
        let scn = preparation
            .scn(&resolver, &scn_client, &oids, attempt)
            .await?;
        let items = preparation
            .action("Resolving OIDs", async {
                resolver
                    .resolve_set(items, scn.as_ref())
                    .map_err(ScenarioBuildError::from)
            })
            .await?;
        Ok((client, items))
    }
    .await
}

#[cfg(test)]
mod tests {
    use super::super::ScenarioReport;
    use super::*;

    fn settings() -> AttemptConfig {
        AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn activity_is_reported_before_the_action_completes() {
        let (tx, mut rx) = mpsc::channel(16);
        let preparation = Preparation::new(TaskId(7), Generation::first(), tx, settings());
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let mut action = Box::pin(preparation.action("Waiting for configuration", async {
            wait.await.unwrap();
            Ok(())
        }));
        assert!(futures_util::poll!(action.as_mut()).is_pending());
        let ScenarioReport::Progress {
            task_id,
            generation,
            progress,
        } = rx.try_recv().unwrap()
        else {
            panic!("expected preparation activity");
        };
        assert_eq!(task_id, TaskId(7));
        assert_eq!(generation, Generation::first());
        assert_eq!(progress.message, "Waiting for configuration");
        release.send(()).unwrap();
        action.await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_and_panic_preserve_typed_failures() {
        for panics in [false, true] {
            let (tx, mut rx) = mpsc::channel(16);
            let preparation = Preparation::new(TaskId(7), Generation::first(), tx, settings());
            let result = preparation
                .action("Preparing resources", async {
                    assert!(!panics, "preparation test panic");
                    std::future::pending::<()>().await;
                    Ok(())
                })
                .await;
            if panics {
                assert!(
                    matches!(result, Err(ScenarioBuildError::Panicked(reason)) if reason == "preparation test panic")
                );
            } else {
                assert_eq!(result, Err(ScenarioBuildError::Timeout));
            }
            assert!(matches!(rx.try_recv().unwrap(), ScenarioReport::Progress { .. }));
            assert!(rx.try_recv().is_err());
        }
    }
}
