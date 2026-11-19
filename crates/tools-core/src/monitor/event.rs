use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};

use crate::monitor::{
    ApplicationId, OperationContext, OperationReport, TaskDescription, TaskTransition,
    scenario::{Generation, PreparationEvent},
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

/// Task lifecycle, preparation progress, and operation evidence published by the monitor.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EventData {
    /// An actual task state change, also retained in the task snapshot.
    #[non_exhaustive]
    TaskStateChanged {
        /// The previous and current states, typed cause, and optional restart plan.
        transition: Box<TaskTransition>,
    },
    /// The start of an entire algorithm with its retry policy.
    #[non_exhaustive]
    OperationStarted {
        /// The run and configuration to which the operation belongs.
        context: OperationContext,
        /// A human-readable explanation of the algorithm being executed, without credentials.
        purpose: String,
        /// UTC operation start time, distinct from envelope publication time.
        started_at: DateTime<Utc>,
        /// The policy for all attempts of this operation.
        config: AttemptConfig,
    },
    /// A terminal operation report; interrupted reports are not yet published by the runtime.
    #[non_exhaustive]
    OperationFinished {
        /// The original run and applied configuration, even if a new revision was accepted.
        context: OperationContext,
        /// Normal results or explicit evidence of interruption.
        report: Box<OperationReport>,
    },

    /// A newly registered task and its accepted configuration description.
    #[non_exhaustive]
    TaskRegistered {
        /// Captured description for the revision in event metadata, without credentials.
        description: Box<TaskDescription>,
    },
    /// An accepted configuration revision, before any replacement run becomes ready.
    #[non_exhaustive]
    TaskConfigurationChanged {
        /// Captured description for the accepted revision, without credentials.
        description: Box<TaskDescription>,
    },
    /// The task was removed from the monitor.
    TaskRemoved,

    /// Progress and results published immediately by scenario preparation.
    #[non_exhaustive]
    Preparation {
        /// A preparation or stage boundary and its associated evidence.
        event: Box<PreparationEvent>,
    },
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

    fn outcomes(at: DateTime<Utc>, elapsed: Duration) -> Vec<ScenarioOutcome> {
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
            ScenarioOutcome::SnmpGet(report(
                at,
                elapsed,
                PollOutcome::Succeeded(samples),
                vec![AttemptOutcome::TimedOut, AttemptOutcome::Succeeded],
            )),
            ScenarioOutcome::SnmpGet(report(
                at,
                elapsed,
                PollOutcome::RetryLimitReached,
                vec![AttemptOutcome::TimedOut; 3],
            )),
            ScenarioOutcome::SnmpSet(report(
                at,
                elapsed,
                PollOutcome::Succeeded(()),
                vec![AttemptOutcome::Succeeded],
            )),
            ScenarioOutcome::SnmpSet(report(
                at,
                elapsed,
                PollOutcome::RetryLimitReached,
                vec![AttemptOutcome::TimedOut],
            )),
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
            1,
            3,
        )
        .unwrap();
        let description = TaskDescription::from(&config);
        let mut data = vec![
            EventData::TaskRegistered {
                description: Box::new(description.clone()),
            },
            EventData::TaskConfigurationChanged {
                description: Box::new(description),
            },
            EventData::TaskStateChanged {
                transition: Box::new(TaskTransition {
                    at,
                    previous: TaskStatus::Starting,
                    current: TaskStatus::Active,
                    cause: TaskTransitionCause::ScenarioReady,
                    restart: None,
                }),
            },
            EventData::TaskRemoved,
            EventData::OperationFinished {
                context: context(),
                report: Box::new(OperationReport::Completed {
                    outcome: ScenarioOutcome::SnmpSet(report(
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
                    )),
                }),
            },
        ];
        for outcome in outcomes(at, elapsed) {
            data.push(EventData::OperationStarted {
                context: context(),
                purpose: "Read or write configured SNMP OIDs".to_string(),
                started_at: at - elapsed,
                config: AttemptConfig::try_new(elapsed, 1, Duration::ZERO).unwrap(),
            });
            data.push(EventData::OperationFinished {
                context: context(),
                report: Box::new(OperationReport::Completed { outcome }),
            });
        }
        for error in [
            ScenarioBuildError::Timeout,
            ScenarioBuildError::SnmpClientCreate,
            ScenarioBuildError::Other("resolution failed".to_string()),
        ] {
            data.push(EventData::TaskStateChanged {
                transition: Box::new(TaskTransition {
                    at,
                    previous: TaskStatus::Starting,
                    current: TaskStatus::Restarting,
                    cause: TaskTransitionCause::Failure(Box::new(ScenarioFailure::Preparation(
                        error,
                    ))),
                    restart: Some(RestartPlan {
                        attempt: 1,
                        scheduled_at: at + elapsed,
                    }),
                }),
            });
        }
        for stage in [
            PanicStage::Build,
            PanicStage::Execution,
            PanicStage::Runtime,
        ] {
            data.push(EventData::TaskStateChanged {
                transition: Box::new(TaskTransition {
                    at,
                    previous: TaskStatus::Active,
                    current: TaskStatus::Restarting,
                    cause: TaskTransitionCause::Failure(Box::new(ScenarioFailure::Panicked {
                        stage,
                        reason: "panic".to_string(),
                    })),
                    restart: Some(RestartPlan {
                        attempt: 1,
                        scheduled_at: at + elapsed,
                    }),
                }),
            });
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
