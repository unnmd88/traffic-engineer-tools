use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use crate::{
    monitor::task::SpecRevision,
    polling::PollReport,
    snmp::{SnmpError, SnmpOid, SnmpVarbind},
};

use super::{Generation, ScenarioBuildError};

/// The purpose of a transport created while preparing a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportRole {
    /// Reads values, including SCN discovery.
    Read,
    /// Writes values to the device.
    Write,
}

/// One observable action in scenario preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreparationStage {
    /// Creates a transport with the specified purpose.
    CreateTransport {
        /// Whether the transport is used for reading or writing.
        role: TransportRole,
    },
    /// Reads and validates the SCN required by the selected OIDs.
    FetchScn,
    /// Applies profile metadata and constructs final OID instances.
    ResolveOids,
}

/// Data obtained from a successful preparation stage; credentials are excluded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreparationStageData {
    /// The transport is ready locally; this does not establish device reachability.
    TransportCreated {
        /// The transport's remote address.
        target: SocketAddr,
    },
    /// The device's validated SCN.
    ScnFetched {
        /// The SCN in its textual representation.
        scn: String,
    },
    /// The final OID instances that will be used by regular operations.
    OidsResolved {
        /// Resolved OIDs in request order.
        oids: Vec<SnmpOid>,
    },
}

/// The result of a preparation stage, including stages that were not required.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[expect(
    clippy::large_enum_variant,
    reason = "Keep small preparation outcomes inline"
)]
pub enum PreparationStageOutcome {
    /// The stage completed and produced data.
    Succeeded(PreparationStageData),
    /// The stage failed, timed out, or panicked.
    Failed(ScenarioBuildError),
    /// The selected request does not require SCN discovery.
    NotRequired,
}

/// Timing, result, and network evidence for one preparation stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparationStageReport {
    /// The action performed by the stage.
    pub stage: PreparationStage,
    /// UTC start time.
    pub started_at: DateTime<Utc>,
    /// UTC completion time.
    pub finished_at: DateTime<Utc>,
    /// Monotonic offset from preparation start.
    pub started_after: Duration,
    /// Monotonic stage duration.
    pub elapsed: Duration,
    /// The result or original typed failure.
    pub outcome: PreparationStageOutcome,
    /// The SCN request report, including a raw response when one was received.
    ///
    /// Absent for local stages, skipped SCN discovery, or interruption before
    /// polling could return its report.
    pub request: Option<Box<PollReport<SnmpVarbind, SnmpError>>>,
}

/// The complete report of one preparation run, including its completed stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparationReport {
    /// UTC start time.
    pub started_at: DateTime<Utc>,
    /// UTC completion time.
    pub finished_at: DateTime<Utc>,
    /// Total monotonic preparation duration.
    pub elapsed: Duration,
    /// Stages in execution order; later stages are absent after a failure.
    pub stages: Vec<PreparationStageReport>,
    /// Readiness or the error that prevented regular execution.
    pub outcome: Result<(), ScenarioBuildError>,
}

/// Preparation progress emitted as each action starts and finishes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreparationEvent {
    /// Preparation has started for a new scenario generation.
    Started {
        /// UTC start time.
        at: DateTime<Utc>,
    },
    /// A preparation action is about to execute.
    StageStarted {
        /// The action that is starting.
        stage: PreparationStage,
        /// UTC stage start time.
        at: DateTime<Utc>,
        /// Monotonic offset from preparation start.
        started_after: Duration,
    },
    /// A stage completed or was explicitly marked as not required.
    StageFinished {
        /// Timing, result, and optional network evidence.
        report: Box<PreparationStageReport>,
    },
    /// Preparation finished successfully or stopped at a failed stage.
    Finished {
        /// The full preparation report.
        report: Box<PreparationReport>,
    },
}

/// The latest preparation's progress and execution context in a task snapshot.
#[derive(Debug, Clone)]
pub struct PreparationView {
    /// The scenario generation that produced this preparation.
    pub generation: Generation,
    /// The configuration revision used by this preparation.
    pub revision: SpecRevision,
    /// UTC preparation start time.
    pub started_at: DateTime<Utc>,
    /// The action currently in progress, if any.
    pub active_stage: Option<PreparationStage>,
    /// Stages already completed or skipped.
    pub stages: Vec<PreparationStageReport>,
    /// The terminal report, available after preparation finishes.
    pub report: Option<PreparationReport>,
}
