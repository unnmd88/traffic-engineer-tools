use serde_json::{Value, json};
use tools_core::{
    monitor::{
        EventData, Generation, MonitorEvent, OperationCancellationReason, OperationContext,
        OperationId, OperationReport, OperationStatus, ScenarioExecution, ScenarioExecutionError,
        ScenarioFailure, ScenarioOutcome, SpecRevision, TaskStatus, TaskTransitionCause,
    },
    polling::{AttemptOutcome, PollOutcome},
    snmp::{SnmpError, SnmpValue},
};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/monitor_operation.json")).unwrap()
}

fn round_trip(record: Value) -> MonitorEvent {
    let event: MonitorEvent = serde_json::from_value(record.clone()).unwrap();
    assert_eq!(serde_json::to_value(&event).unwrap(), record);
    event
}

fn interrupted_report() -> Value {
    json!({
        "started_at": "2026-11-18T09:00:00Z",
        "finished_at": "2026-11-18T09:00:01.150Z",
        "elapsed": { "secs": 1, "nanos": 150000000 },
        "config": {
            "timeout": { "secs": 1, "nanos": 0 },
            "retries": 1,
            "retry_delay": { "secs": 0, "nanos": 100000000 }
        },
        "attempts": [{
            "started_after": { "secs": 0, "nanos": 0 },
            "elapsed": { "secs": 0, "nanos": 900000000 },
            "outcome": { "Failed": {
                "error": { "Snmp": { "Network": {
                    "target": "127.0.0.1:161",
                    "reason": "connection refused"
                } } },
                "retryable": true
            } }
        }],
        "interrupted_attempt": {
            "started_after": { "secs": 1, "nanos": 0 },
            "elapsed": { "secs": 0, "nanos": 150000000 }
        }
    })
}

#[test]
fn fixture_preserves_operation_identity_attempts_and_raw_results() {
    let event = round_trip(fixture());
    let EventData::OperationFinished {
        context, report, ..
    } = event.data
    else {
        panic!("expected operation completion");
    };
    assert_eq!(
        context,
        OperationContext::new(
            OperationId::new(11),
            Generation::first().next().next(),
            SpecRevision::new(3)
        )
    );
    assert_eq!(context.operation_id.get(), 11);
    assert_eq!(report.status(), OperationStatus::Succeeded);
    let OperationReport::Completed {
        outcome: ScenarioOutcome::SnmpGet(report),
        ..
    } = *report
    else {
        panic!("expected a normal GET report");
    };
    assert_eq!(report.attempt_count(), 2);
    assert_eq!(report.attempts[0].outcome, AttemptOutcome::TimedOut);
    let PollOutcome::Succeeded(samples) = report.outcome else {
        panic!("expected successful samples");
    };
    assert!(
        matches!(&samples[0].raw_value, SnmpValue::OctetString(bytes) if bytes == &[0, 128, 255])
    );
    assert!(matches!(samples[1].raw_value, SnmpValue::Counter64(u64::MAX)));
}

#[test]
fn retained_result_keeps_operation_context_and_uses_report_timing() {
    let event = round_trip(fixture());
    let EventData::OperationFinished {
        context, report, ..
    } = event.data
    else {
        panic!("expected operation result");
    };
    let OperationReport::Completed { outcome, .. } = *report else {
        panic!("expected normal completion");
    };
    let result = ScenarioExecution::new(context, outcome);
    let record = serde_json::to_value(&result).unwrap();
    assert_eq!(record.as_object().unwrap().len(), 2);
    assert_eq!(record["context"]["revision"], 3);
    assert!(record.get("finished_at").is_none());
    assert!(record.get("elapsed").is_none());
    let restored: ScenarioExecution = serde_json::from_value(record).unwrap();
    assert_eq!(restored.context, context);
    let ScenarioOutcome::SnmpGet(report) = &restored.outcome else {
        panic!("expected GET report");
    };
    assert_eq!(restored.finished_at(), report.finished_at);
    assert_eq!(restored.elapsed(), report.elapsed);
}

#[test]
fn normal_failure_remains_an_operation_result_with_its_original_error() {
    let mut record = fixture();
    let report =
        &mut record["data"]["OperationFinished"]["report"]["Completed"]["outcome"]["SnmpGet"];
    report["outcome"] = json!("NonRetryableFailure");
    report["attempts"][1]["outcome"] = json!({ "Failed": {
        "error": { "InvalidOid": "invalid request" },
        "retryable": false
    } });
    let event = round_trip(record);
    let EventData::OperationFinished { report, .. } = event.data else {
        panic!("expected operation completion");
    };
    assert_eq!(report.status(), OperationStatus::Failed);
    let OperationReport::Completed {
        outcome: ScenarioOutcome::SnmpGet(report),
        ..
    } = *report
    else {
        panic!("expected a normal failure report");
    };
    assert_eq!(report.attempt_count(), 2);
    assert!(matches!(report.non_retryable_error(),
        Some(SnmpError::InvalidOid(value)) if value == "invalid request"));
}

#[test]
fn cancelled_report_keeps_original_run_and_distinguishes_the_interrupted_attempt() {
    let mut record = fixture();
    record["metadata"]["revision"] = json!(4);
    record["data"]["OperationFinished"]["report"] = json!({ "Cancelled": {
        "reason": "ConfigurationChanged",
        "report": interrupted_report()
    } });
    let event = round_trip(record);
    let EventData::OperationFinished {
        context, report, ..
    } = event.data
    else {
        panic!("expected operation completion");
    };
    assert_eq!(context.revision, SpecRevision::new(3));
    assert_eq!(report.status(), OperationStatus::Cancelled);
    let OperationReport::Cancelled { reason, report, .. } = *report else {
        panic!("expected cancellation evidence");
    };
    assert_eq!(reason, OperationCancellationReason::ConfigurationChanged);
    assert_eq!(report.attempts.len(), 1);
    assert!(report.interrupted_attempt.is_some());
    assert!(matches!(&report.attempts[0].outcome,
        AttemptOutcome::Failed { error: ScenarioExecutionError::Snmp(SnmpError::Network { reason, .. }), retryable: true }
        if reason == "connection refused"));
}

#[test]
fn panic_during_retry_delay_does_not_invent_an_active_attempt() {
    let mut record = fixture();
    let mut progress = interrupted_report();
    progress["interrupted_attempt"] = Value::Null;
    progress["finished_at"] = json!("2026-11-18T09:00:00.950Z");
    progress["elapsed"] = json!({ "secs": 0, "nanos": 950000000 });
    record["data"]["OperationFinished"]["report"] = json!({ "Panicked": {
        "reason": "unexpected state\nwhile waiting",
        "report": progress
    } });
    let event = round_trip(record);
    let EventData::OperationFinished { report, .. } = event.data else {
        panic!("expected operation completion");
    };
    assert_eq!(report.status(), OperationStatus::Panicked);
    let OperationReport::Panicked { reason, report, .. } = *report else {
        panic!("expected panic evidence");
    };
    assert_eq!(reason, "unexpected state\nwhile waiting");
    assert_eq!(report.attempts.len(), 1);
    assert!(report.interrupted_attempt.is_none());
}

#[test]
fn transition_retains_typed_failure_and_scheduled_restart() {
    let mut record = fixture();
    record["data"] = json!({ "TaskStateChanged": { "transition": {
        "at": "2026-11-18T09:00:01.150Z",
        "previous": "Starting",
        "current": "Restarting",
        "cause": { "Failure": { "Preparation": { "Snmp": { "UnexpectedValueType": {
            "expected": "OctetString", "actual": "Integer"
        } } } } },
        "restart": { "attempt": 2, "scheduled_at": "2026-11-18T09:00:03Z" }
    } } });
    let event = round_trip(record);
    let EventData::TaskStateChanged { transition, .. } = event.data else {
        panic!("expected a state transition");
    };
    assert_eq!(transition.previous, TaskStatus::Starting);
    assert_eq!(transition.current, TaskStatus::Restarting);
    assert!(matches!(
        transition.cause,
        TaskTransitionCause::Failure(failure) if matches!(*failure, ScenarioFailure::Preparation(_))
    ));
    assert_eq!(transition.restart.unwrap().attempt, 2);
}

#[test]
fn operation_start_preserves_its_budget_without_creating_a_result() {
    let mut record = fixture();
    let context = record["data"]["OperationFinished"]["context"].clone();
    let config =
        record["data"]["OperationFinished"]["report"]["Completed"]["outcome"]["SnmpGet"]["config"]
            .clone();
    record["data"] = json!({ "OperationStarted": {
        "context": context,
        "purpose": "Read controller state",
        "started_at": "2026-11-18T09:00:00Z",
        "config": config
    } });
    let event = round_trip(record);
    let EventData::OperationStarted {
        config, purpose, ..
    } = event.data
    else {
        panic!("expected operation start");
    };
    assert_eq!(config.retries(), 1);
    assert_eq!(purpose, "Read controller state");
}

#[test]
fn serialized_event_contains_only_metadata_and_data() {
    let event = round_trip(fixture());
    let record = serde_json::to_value(event).unwrap();
    assert_eq!(record.as_object().unwrap().len(), 2);
    assert!(record.get("metadata").is_some());
    assert!(record.get("data").is_some());
}
