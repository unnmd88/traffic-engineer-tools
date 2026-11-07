use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio::time::Duration;

use crate::monitor::{
    ApplicationId,
    scenario::{
        Generation, PanicStage, ScenarioBuildError, ScenarioExecution, ScenarioExecutionError,
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
        polling::{FatalError, PollErrorContext, Response},
        snmp::{
            SnmpGetSample, business_value::BusinessValue,
            group_state_potok_ug405::GroupStatePotokUg405, oid::SnmpOid, value::SnmpValue,
        },
        stage::Stage,
    };

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
            ScenarioOutcome::SnmpGet(Response::Success {
                timestamp: at,
                attempts: 2,
                errors: vec![PollErrorContext {
                    attempt: 1,
                    elapsed,
                    message: "timeout\nretry".to_string(),
                }],
                elapsed,
                payload: samples,
            }),
            ScenarioOutcome::SnmpGet(Response::NoResponse {
                timestamp: at,
                attempts: 3,
                errors: vec![],
                elapsed,
            }),
            ScenarioOutcome::SnmpSet(Response::Success {
                timestamp: at,
                attempts: 1,
                errors: vec![],
                elapsed,
                payload: (),
            }),
            ScenarioOutcome::SnmpSet(Response::NoResponse {
                timestamp: at,
                attempts: 1,
                errors: vec![],
                elapsed,
            }),
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
                error: ScenarioExecutionError::Polling(FatalError {
                    message: "unsupported value".to_string(),
                }),
                retry_in: None,
            },
        ];
        for outcome in outcomes(at, elapsed) {
            let mut metrics = ExecutionMetrics::default();
            metrics.record(outcome.status(), elapsed);
            data.push(EventData::ExecutionFinished {
                execution: ScenarioExecution {
                    finished_at: at,
                    elapsed,
                    outcome,
                },
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
