use std::net::SocketAddr;

use async_snmp::{ErrorStatus, Pdu, PduType, Value, message::CommunityMessage};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    task::{JoinHandle, yield_now},
    time::{Duration, Instant, advance, timeout},
};

use super::*;
use crate::{
    monitor::{TaskProgress, task::TaskId},
    polling::{AttemptConfig, AttemptOutcome, PollDetails, PollOutcome},
    snmp::{
        RawSnmpOidItem, RawSnmpSetItem, SnmpGetSpec, SnmpSetSpec, business_value::BusinessValue,
        value::SnmpValue,
    },
};

struct Session {
    join: JoinHandle<()>,
    commands: mpsc::Sender<ScenarioCommand>,
    reports: mpsc::Receiver<ScenarioReport>,
    progress: Vec<TaskProgress>,
    operations: Vec<(crate::monitor::OperationContext, chrono::DateTime<chrono::Utc>)>,
}

impl Session {
    async fn next_report(&mut self) -> Option<ScenarioReport> {
        loop {
            match self.reports.recv().await? {
                ScenarioReport::Progress {
                    task_id,
                    generation,
                    progress,
                } => {
                    assert_eq!(task_id, TaskId(7));
                    assert_eq!(generation, Generation::first().next());
                    self.progress.push(progress);
                }
                ScenarioReport::OperationStarted {
                    task_id,
                    context,
                    started_at,
                    ..
                } => {
                    assert_eq!(task_id, TaskId(7));
                    assert_eq!(context.generation, Generation::first().next());
                    assert_eq!(context.revision, crate::monitor::SpecRevision::new(3));
                    assert_eq!(
                        context.operation_id.get(),
                        u64::try_from(self.operations.len()).unwrap()
                    );
                    self.operations.push((context, started_at));
                }
                report => {
                    if let ScenarioReport::Executed { execution, .. }
                    | ScenarioReport::ExecutionFailed { execution, .. } = &report
                    {
                        let (context, started_at) = self.operations.last().unwrap();
                        assert_eq!(&execution.context, context);
                        let report_start = execution.report.started_at;
                        assert_eq!(report_start, *started_at);
                    }
                    return Some(report);
                }
            }
        }
    }

    fn drain_preparation(&mut self) {
        while let Ok(report) = self.reports.try_recv() {
            match report {
                ScenarioReport::Progress { progress, .. } => self.progress.push(progress),
                _ => panic!("scenario became ready before SCN was provided"),
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Get,
    Set,
}

impl Operation {
    fn pdu_type(self) -> PduType {
        match self {
            Self::Get => PduType::GetRequest,
            Self::Set => PduType::SetRequest,
        }
    }
}

fn request(peer: &UdpSocket, profile: Option<&str>, oid: &str) -> SnmpGetSpec {
    SnmpGetSpec::from_raw(
        "127.0.0.1".to_string(),
        peer.local_addr().unwrap().port(),
        "public".to_string(),
        profile.map(str::to_string),
        vec![RawSnmpOidItem {
            name: None,
            oid: oid.to_string(),
        }],
    )
    .unwrap()
}

fn set_request(peer: &UdpSocket, profile: Option<&str>, community_r: Option<&str>) -> SnmpSetSpec {
    let (oid, value, value_type) = match profile {
        Some(_) => ("set_stage", "3", None),
        None => ("1.3.6.1.4.1.999.1.0", "42", Some("integer".to_string())),
    };
    SnmpSetSpec::from_raw(
        "127.0.0.1".to_string(),
        peer.local_addr().unwrap().port(),
        community_r.map(str::to_string),
        "private".to_string(),
        profile.map(str::to_string),
        vec![RawSnmpSetItem {
            name: None,
            oid: oid.to_string(),
            value: value.to_string(),
            value_type,
        }],
    )
    .unwrap()
}

fn attempt(retries: u8) -> AttemptConfig {
    AttemptConfig::try_new(Duration::from_secs(1), retries, Duration::from_secs(1)).unwrap()
}

fn get_scenario(request: SnmpGetSpec, retries: u8) -> ScenarioSpec {
    ScenarioSpec::SnmpGet(
        SnmpGetScenarioSpec::try_new(
            request,
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
            attempt(retries),
        )
        .unwrap(),
    )
}

fn set_scenario(request: SnmpSetSpec, retries: u8) -> ScenarioSpec {
    ScenarioSpec::SnmpSet(
        SnmpSetScenarioSpec::try_new(
            request,
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
            attempt(retries),
        )
        .unwrap(),
    )
}

fn spawn(scenario: ScenarioSpec) -> Session {
    let (report_tx, reports) = mpsc::channel(64);
    let (commands, cmd_rx) = mpsc::channel(8);
    let join = tokio::spawn(run_scenario(
        scenario,
        ScenarioContext {
            task_id: TaskId(7),
            generation: Generation::first().next(),
            revision: crate::monitor::SpecRevision::new(3),
        },
        report_tx,
        cmd_rx,
    ));
    Session {
        join,
        commands,
        reports,
        progress: Vec::new(),
        operations: Vec::new(),
    }
}

async fn start(operation: Operation, retries: u8) -> (Session, UdpSocket) {
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let scenario = match operation {
        Operation::Get => get_scenario(request(&peer, None, "1.3.6.1.2.1.1.1.0"), retries),
        Operation::Set => set_scenario(set_request(&peer, None, None), retries),
    };
    let mut session = spawn(scenario);
    let report = session.next_report().await.unwrap();
    assert!(matches!(report, ScenarioReport::Built { .. }));
    assert_eq!(report.key(), (TaskId(7), Generation::first().next()));
    (session, peer)
}

async fn receive(peer: &UdpSocket) -> (CommunityMessage, SocketAddr) {
    let mut packet = [0; 2048];
    let (size, source) = peer.recv_from(&mut packet).await.unwrap();
    let message = CommunityMessage::decode(packet[..size].to_vec().into()).unwrap();
    (message, source)
}

async fn reply(peer: &UdpSocket, message: CommunityMessage, source: SocketAddr, pdu: Pdu) {
    let response = CommunityMessage::new(message.version, message.community, pdu).encode();
    peer.send_to(&response, source).await.unwrap();
}

async fn stop(session: Session) {
    drop(session.commands);
    timeout(Duration::from_secs(1), session.join)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn waits_for_interval_before_first_request_and_after_execution() {
    for operation in [Operation::Get, Operation::Set] {
        let (mut session, peer) = start(operation, 0).await;
        let started_at = Instant::now();
        let (message, source) = receive(&peer).await;
        assert_eq!(started_at.elapsed(), Duration::from_secs(10));
        let pdu = message.pdu.standard().unwrap();
        assert_eq!(pdu.pdu_type, operation.pdu_type());
        if matches!(operation, Operation::Set) {
            assert_eq!(message.community.as_ref(), b"private");
            assert_eq!(pdu.varbinds[0].oid.to_string(), "1.3.6.1.4.1.999.1.0");
            assert_eq!(pdu.varbinds[0].value, Value::Integer(42));
        }
        let mut response = message.pdu.standard().unwrap().to_response();
        response.varbinds[0].value = Value::Integer(42);
        reply(&peer, message, source, response).await;

        let report = session.next_report().await.unwrap();
        assert_eq!(report.key(), (TaskId(7), Generation::first().next()));
        let ScenarioReport::Executed { execution, .. } = report else {
            panic!("expected execution report");
        };
        match (operation, execution.report.detail().unwrap()) {
            (
                Operation::Get,
                ScenarioOutcome::SnmpGet(PollDetails {
                    outcome: PollOutcome::Succeeded(payload),
                    attempts,
                    ..
                }),
            ) => {
                assert_eq!(attempts.len(), 1);
                assert_eq!(payload[0].oid.to_string(), "1.3.6.1.2.1.1.1.0");
                assert!(matches!(payload[0].raw_value, SnmpValue::Integer(42)));
            }
            (
                Operation::Set,
                ScenarioOutcome::SnmpSet(PollDetails {
                    outcome: PollOutcome::Succeeded(()),
                    attempts,
                    ..
                }),
            ) => {
                assert_eq!(attempts.len(), 1);
            }
            (_, outcome) => panic!("unexpected outcome: {outcome:?}"),
        }

        let finished_at = Instant::now();
        receive(&peer).await;
        assert_eq!(finished_at.elapsed(), Duration::from_secs(10));
        assert!(matches!(session.next_report().await, Some(ScenarioReport::Executed { .. })));
        stop(session).await;
    }
}

#[tokio::test(start_paused = true)]
async fn pause_waits_for_all_attempts_and_resume_starts_a_new_interval() {
    for operation in [Operation::Get, Operation::Set] {
        let (mut session, peer) = start(operation, 1).await;
        receive(&peer).await;
        session.commands.send(ScenarioCommand::Pause).await.unwrap();
        receive(&peer).await;
        let ScenarioReport::Executed { execution, .. } = session.next_report().await.unwrap()
        else {
            panic!("expected execution before pause");
        };
        assert!(execution.elapsed() >= Duration::from_secs(1));
        assert!(matches!(
            (operation, execution.report.detail().unwrap()),
            (
                Operation::Get,
                ScenarioOutcome::SnmpGet(PollDetails { outcome: PollOutcome::RetryLimitReached, attempts, .. })
            ) | (
                Operation::Set,
                ScenarioOutcome::SnmpSet(PollDetails { outcome: PollOutcome::RetryLimitReached, attempts, .. })
            ) if attempts.len() == 2
        ));
        assert!(matches!(session.next_report().await, Some(ScenarioReport::Paused { .. })));
        advance(Duration::from_secs(100)).await;
        session.commands.send(ScenarioCommand::Pause).await.unwrap();
        yield_now().await;
        assert!(session.reports.try_recv().is_err());
        let mut packet = [0; 2048];
        assert_eq!(peer.try_recv(&mut packet).unwrap_err().kind(), std::io::ErrorKind::WouldBlock);

        let resumed_at = Instant::now();
        session
            .commands
            .send(ScenarioCommand::Resume)
            .await
            .unwrap();
        assert!(matches!(session.next_report().await, Some(ScenarioReport::Resumed { .. })));
        let (message, source) = receive(&peer).await;
        assert_eq!(resumed_at.elapsed(), Duration::from_secs(10));
        let response = message.pdu.standard().unwrap().to_response();
        reply(&peer, message, source, response).await;
        assert!(matches!(session.next_report().await, Some(ScenarioReport::Executed { .. })));
        stop(session).await;
    }
}

#[tokio::test(start_paused = true)]
async fn redundant_resume_does_not_postpone_request() {
    for operation in [Operation::Get, Operation::Set] {
        let (session, peer) = start(operation, 0).await;
        let started_at = Instant::now();
        for _ in 0..9 {
            advance(Duration::from_secs(1)).await;
            session
                .commands
                .send(ScenarioCommand::Resume)
                .await
                .unwrap();
            yield_now().await;
        }
        receive(&peer).await;
        assert_eq!(started_at.elapsed(), Duration::from_secs(10));
        stop(session).await;
    }
}

#[tokio::test(start_paused = true)]
async fn closed_channels_end_active_and_paused_scenarios() {
    for operation in [Operation::Get, Operation::Set] {
        for paused in [false, true] {
            for close_reports in [false, true] {
                let (mut session, _peer) = start(operation, 0).await;
                if paused {
                    session.commands.send(ScenarioCommand::Pause).await.unwrap();
                    assert!(matches!(
                        session.next_report().await,
                        Some(ScenarioReport::Paused { .. })
                    ));
                }
                if close_reports {
                    drop(session.reports);
                } else {
                    drop(session.commands);
                }
                timeout(Duration::from_secs(1), session.join)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn resolves_scn_before_reporting_built() {
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut session = spawn(get_scenario(request(&peer, Some("potok_ug405"), "stage"), 0));
    let (message, source) = receive(&peer).await;
    session.drain_preparation();
    assert_eq!(
        session.progress.last().unwrap().message,
        "Reading SCN: selected OIDs require a site identifier"
    );
    let mut response = message.pdu.standard().unwrap().to_response();
    assert_eq!(response.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.5.1.1.2.0");
    response.varbinds[0].value = Value::OctetString(b"CO1".as_slice().into());
    reply(&peer, message, source, response).await;
    assert!(matches!(session.next_report().await, Some(ScenarioReport::Built { .. })));
    assert_eq!(
        session
            .progress
            .iter()
            .map(|progress| progress.message.as_str())
            .collect::<Vec<_>>(),
        [
            "Creating SNMP read client",
            "Reading SCN: selected OIDs require a site identifier",
            "Resolving OIDs"
        ]
    );
    let (message, source) = receive(&peer).await;
    let mut response = message.pdu.standard().unwrap().to_response();
    assert_eq!(
        response.varbinds[0].oid.to_string(),
        "1.3.6.1.4.1.13267.3.2.5.1.1.3.1.3.67.79.49"
    );
    response.varbinds[0].value = Value::OctetString(vec![4].into());
    reply(&peer, message, source, response).await;
    let ScenarioReport::Executed { execution, .. } = session.next_report().await.unwrap() else {
        panic!("expected execution report");
    };
    let ScenarioOutcome::SnmpGet(PollDetails {
        outcome: PollOutcome::Succeeded(payload),
        ..
    }) = execution.report.detail().unwrap()
    else {
        panic!("expected successful GET");
    };
    assert_eq!(payload[0].oid_name.as_deref(), Some("utcReplyGn"));
    assert!(matches!(payload[0].value, Some(BusinessValue::StageUg405 { .. })));
    stop(session).await;
}

#[tokio::test(start_paused = true)]
async fn failed_scn_resolution_ends_scenario_without_built() {
    for operation in [Operation::Get, Operation::Set] {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let scenario = match operation {
            Operation::Get => get_scenario(request(&peer, Some("potok_ug405"), "stage"), 0),
            Operation::Set => set_scenario(set_request(&peer, Some("potok_ug405"), None), 0),
        };
        let mut session = spawn(scenario);
        let (message, source) = receive(&peer).await;
        let mut response = message.pdu.standard().unwrap().to_response();
        response.varbinds[0].value = Value::Integer(42);
        reply(&peer, message, source, response).await;
        assert!(matches!(
            session.next_report().await,
            Some(ScenarioReport::BuildFailed {
                error: ScenarioBuildError::Snmp(crate::snmp::SnmpError::UnexpectedValueType { .. }),
                ..
            })
        ));
        assert_eq!(
            session.progress.last().unwrap().message,
            "Reading SCN: selected OIDs require a site identifier"
        );
        assert!(session.next_report().await.is_none());
        session.join.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_protocol_error_preserves_the_complete_report_before_ending_scenario() {
    for operation in [Operation::Get, Operation::Set] {
        let (mut session, peer) = start(operation, 1).await;
        receive(&peer).await;
        let (message, source) = receive(&peer).await;
        let response = message
            .pdu
            .standard()
            .unwrap()
            .to_error_response(ErrorStatus::GenErr, 1);
        reply(&peer, message, source, response).await;
        let Some(ScenarioReport::ExecutionFailed {
            execution, error, ..
        }) = session.next_report().await
        else {
            panic!("expected a failure with the operation report");
        };
        assert!(matches!(
            error,
            ScenarioExecutionError::Snmp(crate::snmp::SnmpError::Protocol { index: 1, .. })
        ));
        let (attempts, finished_at, elapsed) = match execution.report.detail().unwrap() {
            ScenarioOutcome::SnmpGet(report) => {
                (&report.attempts, execution.report.finished_at, execution.report.elapsed)
            }
            ScenarioOutcome::SnmpSet(report) => {
                (&report.attempts, execution.report.finished_at, execution.report.elapsed)
            }
        };
        assert_eq!(execution.finished_at(), finished_at);
        assert_eq!(execution.elapsed(), elapsed);
        assert_eq!(attempts.len(), 2);
        assert!(matches!(
            attempts[0].outcome,
            AttemptOutcome::TimedOut
                | AttemptOutcome::Failed {
                    error: crate::snmp::SnmpError::Timeout { .. },
                    retryable: true
                }
        ));
        assert!(matches!(
            &attempts[1].outcome,
            AttemptOutcome::Failed {
                error: crate::snmp::SnmpError::Protocol { index: 1, .. },
                retryable: false
            }
        ));
        assert!(attempts[1].started_after >= attempts[0].elapsed + Duration::from_secs(1));
        assert!(session.next_report().await.is_none());
        session.join.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn parser_panic_is_reported_as_execution_panic() {
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let spec = request(&peer, None, "1.3.6.1.2.1.1.1.0");
    let mut oids = spec.oids().to_vec();
    oids[0].parser = Some(|_| panic!("test parser panic"));
    let spec = SnmpGetSpec::new(
        SocketAddr::new(spec.host(), spec.port()),
        spec.community().clone(),
        spec.profile(),
        oids,
    );
    let mut session = spawn(get_scenario(spec, 0));
    assert!(matches!(session.next_report().await, Some(ScenarioReport::Built { .. })));
    let (message, source) = receive(&peer).await;
    let response = message.pdu.standard().unwrap().to_response();
    reply(&peer, message, source, response).await;
    let ScenarioReport::ExecutionPanicked { reason, .. } = session.next_report().await.unwrap()
    else {
        panic!("expected execution panic report");
    };
    assert_eq!(reason, "test parser panic");
    assert!(session.next_report().await.is_none());
    session.join.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn set_resolves_scn_with_read_community_and_sends_with_write_community() {
    for community_r in [Some("public"), None] {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let scenario = set_scenario(set_request(&peer, Some("potok_ug405"), community_r), 0);
        let mut session = spawn(scenario);
        let (message, source) = receive(&peer).await;
        session.drain_preparation();
        assert_eq!(
            session.progress.last().unwrap().message,
            "Reading SCN: selected OIDs require a site identifier"
        );
        let read_transports = session
            .progress
            .iter()
            .filter(|progress| progress.message == "Creating SNMP read client")
            .count();
        assert_eq!(read_transports, usize::from(community_r.is_some()));
        assert_eq!(message.community.as_ref(), community_r.unwrap_or("private").as_bytes());
        let mut response = message.pdu.standard().unwrap().to_response();
        assert_eq!(response.pdu_type, PduType::Response);
        assert_eq!(message.pdu.standard().unwrap().pdu_type, PduType::GetRequest);
        assert_eq!(response.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.5.1.1.2.0");
        response.varbinds[0].value = Value::OctetString(b"CO1".as_slice().into());
        reply(&peer, message, source, response).await;
        assert!(matches!(session.next_report().await, Some(ScenarioReport::Built { .. })));

        let built_at = Instant::now();
        assert_eq!(session.progress.first().unwrap().message, "Creating SNMP write client");
        assert_eq!(session.progress.last().unwrap().message, "Resolving OIDs");
        assert_eq!(session.progress.len(), 3 + usize::from(community_r.is_some()));
        let (message, source) = receive(&peer).await;
        assert_eq!(built_at.elapsed(), Duration::from_secs(10));
        assert_eq!(message.community.as_ref(), b"private");
        let pdu = message.pdu.standard().unwrap();
        assert_eq!(pdu.pdu_type, PduType::SetRequest);
        assert_eq!(pdu.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.4.2.1.5.1.3.67.79.49");
        assert_eq!(pdu.varbinds[0].value, Value::OctetString(vec![4].into()));
        let response = pdu.to_response();
        reply(&peer, message, source, response).await;
        let ScenarioReport::Executed { execution, .. } = session.next_report().await.unwrap()
        else {
            panic!("expected execution report");
        };
        assert!(matches!(
            execution.report.detail().unwrap(),
            ScenarioOutcome::SnmpSet(PollDetails {
                outcome: PollOutcome::Succeeded(()),
                ..
            })
        ));
        stop(session).await;
    }
}

#[tokio::test(start_paused = true)]
async fn scenario_clone_resolves_scn_independently_for_each_run() {
    for operation in [Operation::Get, Operation::Set] {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let scenario = match operation {
            Operation::Get => get_scenario(request(&peer, Some("potok_ug405"), "stage"), 0),
            Operation::Set => set_scenario(set_request(&peer, Some("potok_ug405"), None), 0),
        };
        for (scenario, scn, index) in [
            (scenario.clone(), "CO1", "1.3.67.79.49"),
            (scenario, "CO2", "1.3.67.79.50"),
        ] {
            let mut session = spawn(scenario);
            let (message, source) = receive(&peer).await;
            let mut response = message.pdu.standard().unwrap().to_response();
            response.varbinds[0].value = Value::OctetString(scn.as_bytes().to_vec().into());
            reply(&peer, message, source, response).await;
            assert!(matches!(session.next_report().await, Some(ScenarioReport::Built { .. })));

            let (message, source) = receive(&peer).await;
            let pdu = message.pdu.standard().unwrap();
            assert_eq!(pdu.pdu_type, operation.pdu_type());
            let base_oid = match operation {
                Operation::Get => "1.3.6.1.4.1.13267.3.2.5.1.1.3",
                Operation::Set => "1.3.6.1.4.1.13267.3.2.4.2.1.5",
            };
            assert_eq!(pdu.varbinds[0].oid.to_string(), format!("{base_oid}.{index}"));
            let mut response = pdu.to_response();
            response.varbinds[0].value = Value::OctetString(vec![4].into());
            reply(&peer, message, source, response).await;
            assert!(matches!(session.next_report().await, Some(ScenarioReport::Executed { .. })));
            stop(session).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn set_retries_after_timeout_and_stops_on_success() {
    let (mut session, peer) = start(Operation::Set, 2).await;
    receive(&peer).await;
    let (message, source) = receive(&peer).await;
    let response = message.pdu.standard().unwrap().to_response();
    reply(&peer, message, source, response).await;
    let ScenarioReport::Executed { execution, .. } = session.next_report().await.unwrap() else {
        panic!("expected execution report");
    };
    let ScenarioOutcome::SnmpSet(PollDetails {
        outcome: PollOutcome::Succeeded(()),
        attempts,
        ..
    }) = execution.report.detail().unwrap()
    else {
        panic!("expected successful SET");
    };
    assert_eq!(attempts.len(), 2);
    assert!(matches!(
        attempts[0].outcome,
        AttemptOutcome::TimedOut
            | AttemptOutcome::Failed {
                error: crate::snmp::SnmpError::Timeout { .. },
                retryable: true
            }
    ));
    assert!(matches!(attempts[1].outcome, AttemptOutcome::Succeeded));
    stop(session).await;
}
