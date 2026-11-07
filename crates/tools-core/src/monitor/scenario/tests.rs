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
    monitor::{
        probe::{RawSnmpOidItem, RawSnmpSetItem, SnmpGetSpec, SnmpSetSpec},
        task::TaskId,
    },
    polling::{AttemptConfig, Response},
    snmp::{business_value::BusinessValue, value::SnmpValue},
};

struct Session {
    join: JoinHandle<()>,
    commands: mpsc::Sender<ScenarioCommand>,
    reports: mpsc::Receiver<ScenarioReport>,
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
    let (report_tx, reports) = mpsc::channel(8);
    let (commands, cmd_rx) = mpsc::channel(8);
    let join = tokio::spawn(run_scenario(
        scenario,
        TaskId(7),
        Generation::first().next(),
        report_tx,
        cmd_rx,
    ));
    Session {
        join,
        commands,
        reports,
    }
}

async fn start(operation: Operation, retries: u8) -> (Session, UdpSocket) {
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let scenario = match operation {
        Operation::Get => get_scenario(request(&peer, None, "1.3.6.1.2.1.1.1.0"), retries),
        Operation::Set => set_scenario(set_request(&peer, None, None), retries),
    };
    let mut session = spawn(scenario);
    let report = session.reports.recv().await.unwrap();
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

        let report = session.reports.recv().await.unwrap();
        assert_eq!(report.key(), (TaskId(7), Generation::first().next()));
        let ScenarioReport::Executed { execution, .. } = report else {
            panic!("expected execution report");
        };
        match (operation, execution.outcome) {
            (
                Operation::Get,
                ScenarioOutcome::SnmpGet(Response::Success {
                    payload,
                    attempts: 1,
                    ..
                }),
            ) => {
                assert_eq!(payload[0].oid.to_string(), "1.3.6.1.2.1.1.1.0");
                assert!(matches!(payload[0].raw_value, SnmpValue::Integer(42)));
            }
            (Operation::Set, ScenarioOutcome::SnmpSet(Response::Success { attempts: 1, .. })) => {}
            (_, outcome) => panic!("unexpected outcome: {outcome:?}"),
        }

        let finished_at = Instant::now();
        receive(&peer).await;
        assert_eq!(finished_at.elapsed(), Duration::from_secs(10));
        assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Executed { .. })));
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
        let ScenarioReport::Executed { execution, .. } = session.reports.recv().await.unwrap()
        else {
            panic!("expected execution before pause");
        };
        assert!(execution.elapsed >= Duration::from_secs(1));
        assert!(matches!(
            (operation, execution.outcome),
            (
                Operation::Get,
                ScenarioOutcome::SnmpGet(Response::NoResponse { attempts: 2, .. })
            ) | (
                Operation::Set,
                ScenarioOutcome::SnmpSet(Response::NoResponse { attempts: 2, .. })
            )
        ));
        assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Paused { .. })));
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
        assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Resumed { .. })));
        let (message, source) = receive(&peer).await;
        assert_eq!(resumed_at.elapsed(), Duration::from_secs(10));
        let response = message.pdu.standard().unwrap().to_response();
        reply(&peer, message, source, response).await;
        assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Executed { .. })));
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
                        session.reports.recv().await,
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
    assert!(session.reports.try_recv().is_err());
    let mut response = message.pdu.standard().unwrap().to_response();
    assert_eq!(response.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.5.1.1.2.0");
    response.varbinds[0].value = Value::OctetString(b"CO1".as_slice().into());
    reply(&peer, message, source, response).await;
    assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Built { .. })));

    let (message, source) = receive(&peer).await;
    let mut response = message.pdu.standard().unwrap().to_response();
    assert_eq!(
        response.varbinds[0].oid.to_string(),
        "1.3.6.1.4.1.13267.3.2.5.1.1.3.1.3.67.79.49"
    );
    response.varbinds[0].value = Value::OctetString(vec![4].into());
    reply(&peer, message, source, response).await;
    let ScenarioReport::Executed { execution, .. } = session.reports.recv().await.unwrap() else {
        panic!("expected execution report");
    };
    let ScenarioOutcome::SnmpGet(Response::Success { payload, .. }) = execution.outcome else {
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
            session.reports.recv().await,
            Some(ScenarioReport::BuildFailed {
                error: ScenarioBuildError::Other(_),
                ..
            })
        ));
        assert!(session.reports.recv().await.is_none());
        session.join.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_protocol_error_ends_scenario_without_execution_report() {
    for operation in [Operation::Get, Operation::Set] {
        let (mut session, peer) = start(operation, 1).await;
        let (message, source) = receive(&peer).await;
        let response = message
            .pdu
            .standard()
            .unwrap()
            .to_error_response(ErrorStatus::GenErr, 1);
        reply(&peer, message, source, response).await;
        assert!(matches!(
            session.reports.recv().await,
            Some(ScenarioReport::ExecutionFailed { .. })
        ));
        assert!(session.reports.recv().await.is_none());
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
    assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Built { .. })));
    let (message, source) = receive(&peer).await;
    let response = message.pdu.standard().unwrap().to_response();
    reply(&peer, message, source, response).await;
    let ScenarioReport::ExecutionPanicked { reason, .. } = session.reports.recv().await.unwrap()
    else {
        panic!("expected execution panic report");
    };
    assert_eq!(reason, "test parser panic");
    assert!(session.reports.recv().await.is_none());
    session.join.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn set_resolves_scn_with_read_community_and_sends_with_write_community() {
    for community_r in [Some("public"), None] {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let scenario = set_scenario(set_request(&peer, Some("potok_ug405"), community_r), 0);
        let mut session = spawn(scenario);
        let (message, source) = receive(&peer).await;
        assert!(session.reports.try_recv().is_err());
        assert_eq!(message.community.as_ref(), community_r.unwrap_or("private").as_bytes());
        let mut response = message.pdu.standard().unwrap().to_response();
        assert_eq!(response.pdu_type, PduType::Response);
        assert_eq!(message.pdu.standard().unwrap().pdu_type, PduType::GetRequest);
        assert_eq!(response.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.5.1.1.2.0");
        response.varbinds[0].value = Value::OctetString(b"CO1".as_slice().into());
        reply(&peer, message, source, response).await;
        assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Built { .. })));

        let built_at = Instant::now();
        let (message, source) = receive(&peer).await;
        assert_eq!(built_at.elapsed(), Duration::from_secs(10));
        assert_eq!(message.community.as_ref(), b"private");
        let pdu = message.pdu.standard().unwrap();
        assert_eq!(pdu.pdu_type, PduType::SetRequest);
        assert_eq!(pdu.varbinds[0].oid.to_string(), "1.3.6.1.4.1.13267.3.2.4.2.1.5.1.3.67.79.49");
        assert_eq!(pdu.varbinds[0].value, Value::OctetString(vec![4].into()));
        let response = pdu.to_response();
        reply(&peer, message, source, response).await;
        let ScenarioReport::Executed { execution, .. } = session.reports.recv().await.unwrap()
        else {
            panic!("expected execution report");
        };
        assert!(matches!(execution.outcome, ScenarioOutcome::SnmpSet(Response::Success { .. })));
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
            assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Built { .. })));

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
            assert!(matches!(session.reports.recv().await, Some(ScenarioReport::Executed { .. })));
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
    let ScenarioReport::Executed { execution, .. } = session.reports.recv().await.unwrap() else {
        panic!("expected execution report");
    };
    let ScenarioOutcome::SnmpSet(Response::Success {
        attempts, errors, ..
    }) = execution.outcome
    else {
        panic!("expected successful SET");
    };
    assert_eq!(attempts, 2);
    assert_eq!(errors.len(), 1);
    stop(session).await;
}
