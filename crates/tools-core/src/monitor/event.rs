use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};

use crate::monitor::{
    ApplicationId, OperationContext, OperationReport, TaskDescription, TaskProgress,
    TaskTransition,
    scenario::Generation,
    task::{SpecRevision, TaskId},
};
use crate::polling::AttemptConfig;

/// Ordering and task context assigned by the monitor when publishing an event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventMeta {
    /// Zero-based event sequence number within this monitor instance.
    pub seq: u64,
    /// UTC publication time; action timing belongs to the operation report.
    pub at: DateTime<Utc>,
    /// The monitor instance that published the event.
    pub application_id: ApplicationId,
    /// The task within that monitor instance.
    pub task_id: TaskId,
    /// The task's latest run generation; task-level events use zero before the first run.
    pub generation: Generation,
    /// The task revision at publication; operation context retains its applied revision.
    pub revision: SpecRevision,
}

/// The start of an operation, before its first attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OperationStarted {
    /// The run and configuration applied to this operation.
    pub context: OperationContext,
    /// A human-readable explanation, without credentials.
    pub purpose: String,
    /// Source UTC start time, distinct from publication time.
    pub started_at: DateTime<Utc>,
    /// The policy used by this polling operation.
    pub config: AttemptConfig,
}

/// The terminal report of an operation with its original execution context.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OperationFinished {
    /// The operation's original run and applied configuration revision.
    pub context: OperationContext,
    /// Common timing and the scenario-specific completion evidence.
    pub report: Box<OperationReport>,
}

/// Task lifecycle, preparation activity, and operation evidence.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventData {
    /// An actual task state change, also retained in the task snapshot.
    TaskStateChanged(Box<TaskTransition>),
    /// Scenario-authored activity; its text is not a stable machine identifier.
    Preparation(TaskProgress),
    /// An operation is starting with its captured context and policy.
    OperationStarted(Box<OperationStarted>),
    /// An operation terminated; runtime interruption reporting remains future work.
    OperationFinished(OperationFinished),
    /// A newly registered task and its accepted configuration description.
    TaskRegistered(Box<TaskDescription>),
    /// An accepted configuration revision, before replacement execution is ready.
    TaskConfigurationChanged(Box<TaskDescription>),
    /// The task was removed from the monitor.
    TaskRemoved,
}

/// A public event with publication context and a typed payload.
///
/// Normal operation results are published; cancellation and panic reports remain pending.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorEvent {
    /// Publication ordering, time, and task context.
    pub metadata: EventMeta,
    /// The lifecycle change or execution evidence.
    pub data: EventData,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::{
            OutcomeStatus, PanicStage, PollSchedule, RestartPlan, ScenarioBuildError,
            ScenarioFailure, ScenarioOutcome, ScenarioSpec, TaskConfig, TaskStatus,
            TaskTransitionCause,
        },
        polling::{AttemptConfig, AttemptOutcome, AttemptReport, PollOutcome, PollReport},
        snmp::{
            SnmpError, SnmpGetSample, business_value::BusinessValue,
            group_state_potok_ug405::GroupStatePotokUg405, oid::SnmpOid, value::SnmpValue,
        },
        stage::Stage,
    };
    use tokio::time::Duration;

    fn context() -> OperationContext {
        OperationContext::new(
            crate::monitor::OperationId::new(0),
            Generation::first(),
            SpecRevision::new(1),
        )
    }

    fn report<T>(
        at: DateTime<Utc>,
        elapsed: Duration,
        outcome: PollOutcome<T>,
        attempts: Vec<AttemptOutcome<SnmpError>>,
    ) -> PollReport<T, SnmpError> {
        let count = u32::try_from(attempts.len()).unwrap();
        PollReport {
            started_at: at - elapsed,
            finished_at: at,
            elapsed,
            config: AttemptConfig::try_new(
                elapsed,
                u8::try_from(count - 1).unwrap(),
                Duration::ZERO,
            )
            .unwrap(),
            attempts: attempts
                .into_iter()
                .enumerate()
                .map(|(index, outcome)| AttemptReport {
                    started_after: elapsed / count * u32::try_from(index).unwrap(),
                    elapsed: elapsed / count,
                    outcome,
                })
                .collect(),
            outcome,
        }
    }

    fn outcomes(at: DateTime<Utc>, elapsed: Duration) -> Vec<OperationReport> {
        let samples = vec![
            SnmpGetSample {
                oid_name: Some("stage".to_string()),
                oid: SnmpOid::parse("1.3.6.1.2.1.1.1.0").unwrap(),
                value: Some(BusinessValue::Stage(Stage::new(3))),
                raw_value: SnmpValue::OctetString(vec![0, 128, 255]),
            },
            SnmpGetSample {
                oid_name: None,
                oid: SnmpOid::parse("1.3.6.1.2.1.1.2.0").unwrap(),
                value: Some(BusinessValue::GroupStatePotokUg405 {
                    groups: vec![GroupStatePotokUg405::from_byte(1, 9)],
                }),
                raw_value: SnmpValue::Counter64(u64::MAX),
            },
        ];
        vec![
            OperationReport::from_poll(
                report(
                    at,
                    elapsed,
                    PollOutcome::Succeeded(samples),
                    vec![AttemptOutcome::TimedOut, AttemptOutcome::Succeeded],
                ),
                ScenarioOutcome::SnmpGet,
            ),
            OperationReport::from_poll(
                report(
                    at,
                    elapsed,
                    PollOutcome::RetryLimitReached,
                    vec![AttemptOutcome::TimedOut; 3],
                ),
                ScenarioOutcome::SnmpGet,
            ),
            OperationReport::from_poll(
                report(at, elapsed, PollOutcome::Succeeded(()), vec![AttemptOutcome::Succeeded]),
                ScenarioOutcome::SnmpSet,
            ),
            OperationReport::from_poll(
                report(at, elapsed, PollOutcome::RetryLimitReached, vec![AttemptOutcome::TimedOut]),
                ScenarioOutcome::SnmpSet,
            ),
        ]
    }

    fn event_data(at: DateTime<Utc>, elapsed: Duration) -> Vec<EventData> {
        let config = TaskConfig::try_new(
            "request".to_string(),
            ScenarioSpec::snmp_get(
                crate::snmp::SnmpGetSpec::new(
                    "127.0.0.1:161".parse().unwrap(),
                    "public".parse().unwrap(),
                    None,
                    vec![],
                ),
                PollSchedule::fixed_delay(elapsed).unwrap(),
                AttemptConfig::try_new(elapsed, 1, Duration::ZERO).unwrap(),
            )
            .unwrap(),
            3,
        )
        .unwrap();
        let description = TaskDescription::from(&config);
        let mut data = vec![
            EventData::Preparation(TaskProgress {
                at,
                message: "Reading SCN\nwaiting for \"response\"".to_string(),
            }),
            EventData::TaskRegistered(Box::new(description.clone())),
            EventData::TaskConfigurationChanged(Box::new(description)),
            EventData::TaskStateChanged(Box::new(TaskTransition {
                at,
                previous: TaskStatus::Preparing,
                current: TaskStatus::Active,
                cause: TaskTransitionCause::ScenarioReady,
                run_limit: None,
                restart: None,
            })),
            EventData::TaskRemoved,
            EventData::OperationFinished(OperationFinished {
                context: context(),
                report: Box::new(OperationReport::from_poll(
                    report(
                        at,
                        elapsed,
                        PollOutcome::NonRetryableFailure,
                        vec![
                            AttemptOutcome::TimedOut,
                            AttemptOutcome::Failed {
                                error: SnmpError::UnsupportedForSet {
                                    value: "unsupported value".to_string(),
                                },
                                retryable: false,
                            },
                        ],
                    ),
                    ScenarioOutcome::SnmpSet,
                )),
            }),
        ];
        for outcome in outcomes(at, elapsed) {
            data.push(EventData::OperationStarted(Box::new(OperationStarted {
                context: context(),
                purpose: "Read or write configured SNMP OIDs".to_string(),
                started_at: at - elapsed,
                config: AttemptConfig::try_new(elapsed, 1, Duration::ZERO).unwrap(),
            })));
            data.push(EventData::OperationFinished(OperationFinished {
                context: context(),
                report: Box::new(outcome),
            }));
        }
        for error in [
            ScenarioBuildError::Timeout,
            ScenarioBuildError::SnmpClientCreate,
            ScenarioBuildError::Other("resolution failed".to_string()),
        ] {
            data.push(EventData::TaskStateChanged(Box::new(TaskTransition {
                at,
                previous: TaskStatus::Preparing,
                current: TaskStatus::Restarting,
                cause: TaskTransitionCause::Failure(Box::new(ScenarioFailure::Preparation(error))),
                run_limit: None,
                restart: Some(RestartPlan {
                    attempt: 1,
                    scheduled_at: at + elapsed,
                }),
            })));
        }
        for stage in [
            PanicStage::Build,
            PanicStage::Execution,
            PanicStage::Runtime,
        ] {
            data.push(EventData::TaskStateChanged(Box::new(TaskTransition {
                at,
                previous: TaskStatus::Active,
                current: TaskStatus::Restarting,
                cause: TaskTransitionCause::Failure(Box::new(ScenarioFailure::Panicked {
                    stage,
                    reason: "panic".to_string(),
                })),
                run_limit: None,
                restart: Some(RestartPlan {
                    attempt: 1,
                    scheduled_at: at + elapsed,
                }),
            })));
        }
        data
    }

    #[test]
    fn events_round_trip_with_results_errors_and_binary_values() {
        let at = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let elapsed = Duration::new(2, 123_456_789);
        let application_id = ApplicationId::generate();
        for (seq, data) in event_data(at, elapsed).into_iter().enumerate() {
            let event = MonitorEvent {
                metadata: EventMeta {
                    seq: seq as u64,
                    at,
                    application_id,
                    task_id: TaskId(7),
                    generation: Generation::first().next(),
                    revision: SpecRevision::new(3),
                },
                data,
            };
            let json = serde_json::to_value(&event).unwrap();
            assert_eq!(json["metadata"]["application_id"], application_id.to_string());
            assert_eq!(json["metadata"]["generation"], 1);
            let decoded: MonitorEvent = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(decoded.metadata, event.metadata);
            assert_eq!(serde_json::to_value(decoded).unwrap(), json);
        }
        let status = serde_json::to_string(&OutcomeStatus::Degraded).unwrap();
        assert_eq!(
            serde_json::from_str::<OutcomeStatus>(&status).unwrap(),
            OutcomeStatus::Degraded
        );
    }
}
