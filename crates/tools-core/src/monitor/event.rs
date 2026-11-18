use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::monitor::{
    ApplicationId,
    scenario::{
        Generation, PanicStage, PreparationEvent, ScenarioBuildError, ScenarioExecution,
        ScenarioExecutionError,
    },
    task::{ExecutionMetrics, SpecRevision, TaskId},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventMeta {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub application_id: ApplicationId,
    pub task_id: TaskId,
    pub generation: Generation,
    pub revision: SpecRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EventData {
    TaskAdded,
    TaskSpecUpdated,
    #[non_exhaustive]
    TaskStarting {
        attempt: u32,
    },
    TaskStarted,
    TaskPaused,
    TaskResumed,
    TaskStopped,
    TaskCompleted,
    TaskRemoved,

    /// Progress and results published immediately by scenario preparation.
    #[non_exhaustive]
    Preparation {
        /// A preparation or stage boundary and its associated evidence.
        event: Box<PreparationEvent>,
    },

    #[non_exhaustive]
    ExecutionFinished {
        execution: ScenarioExecution,
        metrics: ExecutionMetrics,
    },

    #[non_exhaustive]
    BuildFailed {
        error: ScenarioBuildError,
        retry_in: Option<Duration>,
    },
    #[non_exhaustive]
    ExecutionFailed {
        error: ScenarioExecutionError,
        /// The complete operation report, including attempts before the failure.
        execution: ScenarioExecution,
        retry_in: Option<Duration>,
    },
    #[non_exhaustive]
    ScenarioPanicked {
        stage: PanicStage,
        reason: String,
        retry_in: Option<Duration>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorEvent {
    pub metadata: EventMeta,
    pub data: EventData,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        monitor::{OutcomeStatus, ScenarioOutcome},
        polling::{AttemptConfig, AttemptOutcome, AttemptReport, PollOutcome, PollReport},
        snmp::{
            SnmpError, SnmpGetSample, business_value::BusinessValue,
            group_state_potok_ug405::GroupStatePotokUg405, oid::SnmpOid, value::SnmpValue,
        },
        stage::Stage,
    };

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
        let mut data = vec![
            EventData::TaskAdded,
            EventData::TaskSpecUpdated,
            EventData::TaskStarting { attempt: 2 },
            EventData::TaskStarted,
            EventData::TaskPaused,
            EventData::TaskResumed,
            EventData::TaskStopped,
            EventData::TaskCompleted,
            EventData::TaskRemoved,
            EventData::ExecutionFailed {
                error: ScenarioExecutionError::Snmp(SnmpError::UnsupportedForSet {
                    value: "unsupported value".to_string(),
                }),
                execution: ScenarioExecution::new(ScenarioOutcome::SnmpSet(report(
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
                ))),
                retry_in: None,
            },
        ];
        for outcome in outcomes(at, elapsed) {
            let mut metrics = ExecutionMetrics::default();
            metrics.record(outcome.status(), elapsed);
            data.push(EventData::ExecutionFinished {
                execution: ScenarioExecution::new(outcome),
                metrics,
            });
        }
        for error in [
            ScenarioBuildError::Timeout,
            ScenarioBuildError::SnmpClientCreate,
            ScenarioBuildError::Other("resolution failed".to_string()),
        ] {
            data.push(EventData::BuildFailed {
                error,
                retry_in: Some(elapsed),
            });
        }
        for stage in [
            PanicStage::Build,
            PanicStage::Execution,
            PanicStage::Runtime,
        ] {
            data.push(EventData::ScenarioPanicked {
                stage,
                reason: "panic".to_string(),
                retry_in: Some(elapsed),
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
