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
    monitor::task::TaskId,
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
    Generation, PreparationEvent, PreparationReport, PreparationStage, PreparationStageData,
    PreparationStageOutcome, PreparationStageReport, ScenarioBuildError, TransportRole,
    runtime::{
        ScenarioReport, build_timeout, classify_error, connect, panic_message, transport_timeout,
    },
};

type ScnRequestReport = PollReport<SnmpVarbind, SnmpError>;

struct Preparation {
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<ScenarioReport>,
    started_at: chrono::DateTime<Utc>,
    started: Instant,
    deadline: Instant,
    stages: Vec<PreparationStageReport>,
}

impl Preparation {
    async fn start(
        task_id: TaskId,
        generation: Generation,
        reports: mpsc::Sender<ScenarioReport>,
        attempt: AttemptConfig,
    ) -> Self {
        let started = Instant::now();
        let this = Self {
            task_id,
            generation,
            reports,
            started_at: Utc::now(),
            started,
            deadline: started + build_timeout(&attempt),
            stages: Vec::new(),
        };
        this.emit(PreparationEvent::Started {
            at: this.started_at,
        })
        .await;
        this
    }

    async fn emit(&self, event: PreparationEvent) {
        match &event {
            PreparationEvent::Started { at } => tracing::info!(
                task_id = %self.task_id, generation = ?self.generation, at = %at,
                "scenario preparation started"
            ),
            PreparationEvent::StageStarted {
                stage,
                at,
                started_after,
            } => tracing::info!(
                task_id = %self.task_id, generation = ?self.generation, ?stage, at = %at,
                started_after_ms = started_after.as_secs_f64() * 1000.0,
                "preparation stage started"
            ),
            PreparationEvent::StageFinished { report } => match &report.outcome {
                PreparationStageOutcome::Failed(error) => tracing::warn!(
                    task_id = %self.task_id, generation = ?self.generation, stage = ?report.stage,
                    %error, elapsed_ms = report.elapsed.as_secs_f64() * 1000.0,
                    "preparation stage failed"
                ),
                outcome => tracing::info!(
                    task_id = %self.task_id, generation = ?self.generation, stage = ?report.stage,
                    ?outcome, elapsed_ms = report.elapsed.as_secs_f64() * 1000.0,
                    "preparation stage finished"
                ),
            },
            PreparationEvent::Finished { report } => tracing::info!(
                task_id = %self.task_id, generation = ?self.generation,
                outcome = ?report.outcome, stages = report.stages.len(),
                elapsed_ms = report.elapsed.as_secs_f64() * 1000.0,
                "scenario preparation finished"
            ),
        }
        let _ = self
            .reports
            .send(ScenarioReport::Preparation {
                task_id: self.task_id,
                generation: self.generation,
                event: Box::new(event),
            })
            .await;
    }

    async fn stage<T, F, D>(
        &mut self,
        stage: PreparationStage,
        execute: F,
        describe: D,
    ) -> Result<T, ScenarioBuildError>
    where
        F: Future<Output = (Result<T, ScenarioBuildError>, Option<ScnRequestReport>)>,
        D: FnOnce(&T) -> PreparationStageData,
    {
        let started_at = Utc::now();
        let started = Instant::now();
        let started_after = started.duration_since(self.started);
        self.emit(PreparationEvent::StageStarted {
            stage,
            at: started_at,
            started_after,
        })
        .await;
        let executed = AssertUnwindSafe(
            timeout_at(self.deadline, execute)
                .instrument(tracing::info_span!("preparation_stage", task_id = %self.task_id, generation = ?self.generation, ?stage)),
        )
        .catch_unwind()
        .await;
        let (result, request) = match executed {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => (Err(ScenarioBuildError::Timeout), None),
            Err(panic) => (Err(ScenarioBuildError::Panicked(panic_message(panic.as_ref()))), None),
        };
        let report = PreparationStageReport {
            stage,
            started_at,
            finished_at: Utc::now(),
            started_after,
            elapsed: started.elapsed(),
            outcome: match &result {
                Ok(value) => PreparationStageOutcome::Succeeded(describe(value)),
                Err(error) => PreparationStageOutcome::Failed(error.clone()),
            },
            request: request.map(Box::new),
        };
        self.emit(PreparationEvent::StageFinished {
            report: Box::new(report.clone()),
        })
        .await;
        self.stages.push(report);
        result
    }

    async fn transport(
        &mut self,
        target: SocketAddr,
        community: Community,
        role: TransportRole,
        attempt: AttemptConfig,
    ) -> Result<SnmpClient, ScenarioBuildError> {
        self.stage(
            PreparationStage::CreateTransport { role },
            async { (connect(target.ip(), target.port(), community, attempt).await, None) },
            |client| PreparationStageData::TransportCreated {
                target: client.socket_addr(),
            },
        )
        .await
    }

    async fn scn(
        &mut self,
        resolver: &Resolver,
        client: &SnmpClient,
        oids: &[SnmpOid],
        attempt: AttemptConfig,
    ) -> Result<Option<Scn>, ScenarioBuildError> {
        if !resolver.needs_scn(oids) {
            let at = Utc::now();
            let report = PreparationStageReport {
                stage: PreparationStage::FetchScn,
                started_at: at,
                finished_at: at,
                started_after: self.started.elapsed(),
                elapsed: Duration::ZERO,
                outcome: PreparationStageOutcome::NotRequired,
                request: None,
            };
            self.emit(PreparationEvent::StageFinished {
                report: Box::new(report.clone()),
            })
            .await;
            self.stages.push(report);
            return Ok(None);
        }
        let settings = AttemptConfig::try_new(transport_timeout(&attempt), 0, Duration::ZERO)
            .expect("transport timeout is positive");
        self.stage(
            PreparationStage::FetchScn,
            async {
                let request = ScnRequest {
                    client: client.clone(),
                };
                let report = poll_observed(&settings, &request, log_attempt).await;
                let scn = decode_scn(&report);
                (scn, Some(report))
            },
            |scn| PreparationStageData::ScnFetched {
                scn: scn.as_str().to_string(),
            },
        )
        .await
        .map(Some)
    }

    async fn finish(self, outcome: Result<(), ScenarioBuildError>) {
        let report = PreparationReport {
            started_at: self.started_at,
            finished_at: Utc::now(),
            elapsed: self.started.elapsed(),
            stages: self.stages.clone(),
            outcome,
        };
        self.emit(PreparationEvent::Finished {
            report: Box::new(report),
        })
        .await;
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

pub(super) async fn get(
    spec: SnmpGetSpec,
    attempt: AttemptConfig,
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<ScenarioReport>,
) -> Result<(SnmpClient, Vec<SnmpGetItem>), ScenarioBuildError> {
    let mut preparation = Preparation::start(task_id, generation, reports, attempt).await;
    let result = async {
        let (host, port, community, profile, items) = spec.into_parts();
        let client = preparation
            .transport(SocketAddr::new(host, port), community, TransportRole::Read, attempt)
            .await?;
        let resolver = Resolver::new(profile);
        let oids: Vec<_> = items.iter().map(|item| item.oid.clone()).collect();
        let scn = preparation.scn(&resolver, &client, &oids, attempt).await?;
        let items = preparation
            .stage(
                PreparationStage::ResolveOids,
                async {
                    (
                        resolver
                            .resolve_get(items, scn.as_ref())
                            .map_err(ScenarioBuildError::from),
                        None,
                    )
                },
                |items| PreparationStageData::OidsResolved {
                    oids: items.iter().map(|item| item.oid.clone()).collect(),
                },
            )
            .await?;
        Ok((client, items))
    }
    .await;
    preparation
        .finish(result.as_ref().map(|_| ()).map_err(Clone::clone))
        .await;
    result
}

pub(super) async fn set(
    spec: SnmpSetSpec,
    attempt: AttemptConfig,
    task_id: TaskId,
    generation: Generation,
    reports: mpsc::Sender<ScenarioReport>,
) -> Result<(SnmpClient, Vec<SnmpSetItem>), ScenarioBuildError> {
    let mut preparation = Preparation::start(task_id, generation, reports, attempt).await;
    let result = async {
        let (host, port, community_r, community, profile, items) = spec.into_parts();
        let target = SocketAddr::new(host, port);
        let client = preparation
            .transport(target, community, TransportRole::Write, attempt)
            .await?;
        let scn_client = match community_r {
            Some(community) => {
                preparation
                    .transport(target, community, TransportRole::Read, attempt)
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
            .stage(
                PreparationStage::ResolveOids,
                async {
                    (
                        resolver
                            .resolve_set(items, scn.as_ref())
                            .map_err(ScenarioBuildError::from),
                        None,
                    )
                },
                |items| PreparationStageData::OidsResolved {
                    oids: items.iter().map(|item| item.oid.clone()).collect(),
                },
            )
            .await?;
        Ok((client, items))
    }
    .await;
    preparation
        .finish(result.as_ref().map(|_| ()).map_err(Clone::clone))
        .await;
    result
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use super::*;

    fn capture_logs() -> (PathBuf, impl tracing::Subscriber + Send + Sync) {
        let path =
            std::env::temp_dir().join(format!("traffic-preparation-{}.log", uuid::Uuid::new_v4()));
        let file = Arc::new(std::fs::File::create(&path).unwrap());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(file)
            .finish();
        (path, subscriber)
    }

    fn target() -> SocketAddr {
        "127.0.0.1:161".parse().unwrap()
    }

    fn settings() -> AttemptConfig {
        AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap()
    }

    fn transport_data(_: &()) -> PreparationStageData {
        PreparationStageData::TransportCreated { target: target() }
    }

    fn event(report: ScenarioReport) -> PreparationEvent {
        let ScenarioReport::Preparation { event, .. } = report else {
            panic!("expected preparation progress");
        };
        *event
    }

    #[tokio::test(start_paused = true)]
    async fn stage_start_is_published_and_logged_before_the_action_finishes() {
        let (path, subscriber) = capture_logs();
        let _guard = tracing::subscriber::set_default(subscriber);
        let (tx, mut rx) = mpsc::channel(16);
        let mut preparation =
            Preparation::start(TaskId(7), Generation::first(), tx, settings()).await;
        assert!(matches!(event(rx.try_recv().unwrap()), PreparationEvent::Started { .. }));
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let mut stage = Box::pin(preparation.stage(
            PreparationStage::CreateTransport {
                role: TransportRole::Read,
            },
            async {
                wait.await.unwrap();
                (Ok(()), None)
            },
            transport_data,
        ));
        assert!(futures_util::poll!(stage.as_mut()).is_pending());
        assert!(matches!(event(rx.try_recv().unwrap()), PreparationEvent::StageStarted { .. }));
        let logs = std::fs::read_to_string(&path).unwrap();
        assert!(logs.contains("preparation stage started"));
        assert!(!logs.contains("preparation stage finished"));
        release.send(()).unwrap();
        stage.await.unwrap();
        assert!(matches!(event(rx.try_recv().unwrap()), PreparationEvent::StageFinished { .. }));
        preparation.finish(Ok(())).await;
        assert!(
            matches!(event(rx.try_recv().unwrap()), PreparationEvent::Finished { report } if report.outcome.is_ok())
        );
        let logs = std::fs::read_to_string(&path).unwrap();
        assert!(logs.contains("preparation stage finished"));
        assert!(logs.contains("scenario preparation finished"));
        drop(_guard);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn typed_transport_failure_is_logged_and_retained_with_previous_stages() {
        let (path, subscriber) = capture_logs();
        let _guard = tracing::subscriber::set_default(subscriber);
        let (tx, mut rx) = mpsc::channel(16);
        let mut preparation =
            Preparation::start(TaskId(7), Generation::first(), tx, settings()).await;
        preparation
            .stage(
                PreparationStage::CreateTransport {
                    role: TransportRole::Write,
                },
                async { (Ok(()), None) },
                transport_data,
            )
            .await
            .unwrap();
        let error = ScenarioBuildError::Snmp(SnmpError::Network {
            target: target(),
            reason: "socket creation failed".to_string(),
        });
        let result = preparation
            .stage(
                PreparationStage::CreateTransport {
                    role: TransportRole::Read,
                },
                async { (Err::<(), _>(error.clone()), None) },
                transport_data,
            )
            .await;
        assert_eq!(result, Err(error.clone()));
        let logs = std::fs::read_to_string(&path).unwrap();
        assert!(logs.contains("preparation stage failed"));
        assert!(logs.contains("socket creation failed"));
        preparation.finish(result).await;
        let mut summary = None;
        while let Ok(report) = rx.try_recv() {
            if let PreparationEvent::Finished { report } = event(report) {
                summary = Some(report);
            }
        }
        let summary = summary.unwrap();
        assert_eq!(summary.outcome, Err(error));
        assert_eq!(summary.stages.len(), 2);
        assert!(matches!(summary.stages[0].outcome, PreparationStageOutcome::Succeeded(_)));
        assert!(matches!(
            summary.stages[1].outcome,
            PreparationStageOutcome::Failed(ScenarioBuildError::Snmp(SnmpError::Network { .. }))
        ));
        let json = serde_json::to_value(&summary).unwrap();
        let restored: PreparationReport = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), json);
        drop(_guard);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_and_panic_finish_the_active_stage_and_preparation() {
        for panics in [false, true] {
            let (tx, mut rx) = mpsc::channel(16);
            let mut preparation =
                Preparation::start(TaskId(7), Generation::first(), tx, settings()).await;
            let result = preparation
                .stage(
                    PreparationStage::CreateTransport {
                        role: TransportRole::Read,
                    },
                    async {
                        assert!(!panics, "preparation test panic");
                        std::future::pending::<()>().await;
                        (Ok(()), None)
                    },
                    transport_data,
                )
                .await;
            if panics {
                assert!(
                    matches!(&result, Err(ScenarioBuildError::Panicked(reason)) if reason == "preparation test panic")
                );
            } else {
                assert_eq!(result, Err(ScenarioBuildError::Timeout));
            }
            preparation.finish(result).await;
            let mut events = Vec::new();
            while let Ok(report) = rx.try_recv() {
                events.push(event(report));
            }
            assert_eq!(events.len(), 4);
            assert!(
                matches!(&events[2], PreparationEvent::StageFinished { report } if matches!(report.outcome, PreparationStageOutcome::Failed(_)))
            );
            assert!(
                matches!(&events[3], PreparationEvent::Finished { report } if report.outcome.is_err() && report.stages.len() == 1)
            );
        }
    }
}
