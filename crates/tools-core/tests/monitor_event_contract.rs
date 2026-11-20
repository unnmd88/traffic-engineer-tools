use serde_json::{Value, json};
use tools_core::monitor::{OperationFinished, OperationStarted};
use tools_core::{
    monitor::{
        EventData, Generation, MonitorEvent, OperationCancellationReason, OperationCompletion,
        OperationContext, OperationId, OperationStatus, ScenarioExecution, ScenarioExecutionError,
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
    let EventData::OperationFinished(OperationFinished {
        context, report, ..
    }) = event.data
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
    assert_eq!(report.elapsed, std::time::Duration::from_millis(1150));
    assert_eq!(report.started_at.to_rfc3339(), "2026-11-18T09:00:00+00:00");
    assert_eq!(report.finished_at.to_rfc3339(), "2026-11-18T09:00:01.150+00:00");
    let OperationCompletion::Returned { detail } = report.completion else {
        panic!("expected normal completion");
    };
    let ScenarioOutcome::SnmpGet(report) = *detail else {
        panic!("expected GET details");
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
    let EventData::OperationFinished(OperationFinished {
        context, report, ..
    }) = event.data
    else {
        panic!("expected operation result");
    };
    let result = ScenarioExecution::new(context, *report);
    let record = serde_json::to_value(&result).unwrap();
    assert_eq!(record.as_object().unwrap().len(), 2);
    assert_eq!(record["context"]["revision"], 3);
    assert!(record.get("finished_at").is_none());
    assert!(record.get("elapsed").is_none());
    let restored: ScenarioExecution = serde_json::from_value(record).unwrap();
    assert_eq!(restored.context, context);
    assert_eq!(restored.finished_at(), restored.report.finished_at);
    assert_eq!(restored.elapsed(), restored.report.elapsed);
    assert!(matches!(restored.report.detail(), Some(ScenarioOutcome::SnmpGet(_))));
}

#[test]
fn normal_failure_remains_an_operation_result_with_its_original_error() {
    let mut record = fixture();
    let report = &mut record["data"]["report"]["completion"]["detail"]["report"];
    report["outcome"] = json!("NonRetryableFailure");
    report["attempts"][1]["outcome"] = json!({ "Failed": {
        "error": { "InvalidOid": "invalid request" },
        "retryable": false
    } });
    let event = round_trip(record);
    let EventData::OperationFinished(OperationFinished { report, .. }) = event.data else {
        panic!("expected operation completion");
    };
    assert_eq!(report.status(), OperationStatus::Failed);
    let OperationCompletion::Returned { detail } = report.completion else {
        panic!("expected a normal failure report");
    };
    let ScenarioOutcome::SnmpGet(report) = *detail else {
        panic!("expected GET details");
    };
    assert_eq!(report.attempt_count(), 2);
    assert!(matches!(report.non_retryable_error(),
        Some(SnmpError::InvalidOid(value)) if value == "invalid request"));
}

#[test]
fn cancelled_report_keeps_original_run_and_distinguishes_the_interrupted_attempt() {
    let mut record = fixture();
    record["metadata"]["revision"] = json!(4);
    let mut report = interrupted_report();
    let started_at = report
        .as_object_mut()
        .unwrap()
        .remove("started_at")
        .unwrap();
    let finished_at = report
        .as_object_mut()
        .unwrap()
        .remove("finished_at")
        .unwrap();
    let elapsed = report.as_object_mut().unwrap().remove("elapsed").unwrap();
    record["data"]["report"] = json!({
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed": elapsed,
        "completion": { "type": "cancelled", "reason": "ConfigurationChanged", "evidence": report }
    });
    let event = round_trip(record);
    let EventData::OperationFinished(OperationFinished {
        context, report, ..
    }) = event.data
    else {
        panic!("expected operation completion");
    };
    assert_eq!(context.revision, SpecRevision::new(3));
    assert_eq!(report.status(), OperationStatus::Cancelled);
    let OperationCompletion::Cancelled {
        reason,
        evidence: report,
        ..
    } = report.completion
    else {
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
    let started_at = progress
        .as_object_mut()
        .unwrap()
        .remove("started_at")
        .unwrap();
    let finished_at = progress
        .as_object_mut()
        .unwrap()
        .remove("finished_at")
        .unwrap();
    let elapsed = progress.as_object_mut().unwrap().remove("elapsed").unwrap();
    record["data"]["report"] = json!({
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed": elapsed,
        "completion": { "type": "panicked", "reason": "unexpected state\nwhile waiting", "evidence": progress }
    });
    let event = round_trip(record);
    let EventData::OperationFinished(OperationFinished { report, .. }) = event.data else {
        panic!("expected operation completion");
    };
    assert_eq!(report.status(), OperationStatus::Panicked);
    let OperationCompletion::Panicked {
        reason,
        evidence: report,
        ..
    } = report.completion
    else {
        panic!("expected panic evidence");
    };
    assert_eq!(reason.as_ref(), "unexpected state\nwhile waiting");
    assert_eq!(report.attempts.len(), 1);
    assert!(report.interrupted_attempt.is_none());
}

#[test]
fn transition_retains_typed_failure_and_scheduled_restart() {
    let mut record = fixture();
    record["data"] = json!({ "type": "task_state_changed",
        "at": "2026-11-18T09:00:01.150Z",
        "previous": "Preparing",
        "current": "Restarting",
        "cause": { "Failure": { "Preparation": { "Snmp": { "UnexpectedValueType": {
            "expected": "OctetString", "actual": "Integer"
        } } } } },
        "restart": { "attempt": 2, "scheduled_at": "2026-11-18T09:00:03Z" }
    });
    let event = round_trip(record);
    let EventData::TaskStateChanged(transition) = event.data else {
        panic!("expected a state transition");
    };
    assert_eq!(transition.previous, TaskStatus::Preparing);
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
    let context = record["data"]["context"].clone();
    let config = record["data"]["report"]["completion"]["detail"]["report"]["config"].clone();
    record["data"] = json!({ "type": "operation_started",
        "context": context,
        "purpose": "Read controller state",
        "started_at": "2026-11-18T09:00:00Z",
        "config": config
    });
    let event = round_trip(record);
    let EventData::OperationStarted(operation) = event.data else {
        panic!("expected operation start");
    };
    let OperationStarted {
        config, purpose, ..
    } = *operation;
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
    assert_eq!(record["data"]["type"], "operation_finished");
    assert_eq!(record["data"]["report"]["completion"]["type"], "returned");
    let detail = &record["data"]["report"]["completion"]["detail"];
    assert_eq!(detail["type"], "snmp_get");
    for field in ["started_at", "finished_at", "elapsed"] {
        assert!(detail["report"].get(field).is_none());
        assert!(record["data"]["report"].get(field).is_some());
    }
}
