use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{
    monitor::{Generation, OutcomeStatus, ScenarioExecutionError, ScenarioOutcome, SpecRevision},
    polling::{AttemptConfig, AttemptReport},
};

/// Identifies an operation within one scenario generation of a task.
///
/// The complete identity also includes the application, task, and generation.
/// Allocation must be independent of resettable metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an identifier from its run-local number.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the run-local number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Execution context captured for operation events and retained results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OperationContext {
    /// The operation's number within the scenario run.
    pub operation_id: OperationId,
    /// The scenario run that owns the operation.
    pub generation: Generation,
    /// The configuration applied to that run, which may precede the latest revision.
    pub revision: SpecRevision,
}

impl OperationContext {
    /// Captures an operation's identity and applied configuration revision.
    pub const fn new(
        operation_id: OperationId,
        generation: Generation,
        revision: SpecRevision,
    ) -> Self {
        Self {
            operation_id,
            generation,
            revision,
        }
    }
}

/// Why the monitor requested cancellation of an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OperationCancellationReason {
    /// The task was explicitly stopped.
    TaskStopped,
    /// A replacement configuration retired the operation's run.
    ConfigurationChanged,
    /// The task was removed.
    TaskRemoved,
    /// The monitor was shut down.
    Shutdown,
}

/// Timing of an attempt interrupted before it could produce a normal report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InterruptedAttempt {
    /// Monotonic offset from the start of the operation.
    pub started_after: Duration,
    /// Time spent in the attempt before interruption.
    pub elapsed: Duration,
}

/// Available evidence when an operation cannot return its normal result.
///
/// Cancellation and a caught panic are local execution outcomes. Neither establishes
/// whether a device executed an already-sent command. These passive report fields do
/// not validate timing or attempt-order invariants during deserialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InterruptedOperationReport {
    /// UTC operation start time.
    pub started_at: DateTime<Utc>,
    /// UTC time at which local execution terminated.
    pub finished_at: DateTime<Utc>,
    /// Monotonic duration including attempts and retry delays.
    pub elapsed: Duration,
    /// Effective attempt settings.
    pub config: AttemptConfig,
    /// Completed attempts in chronological order, preserving typed adapter errors.
    pub attempts: Vec<AttemptReport<ScenarioExecutionError>>,
    /// The interrupted attempt, absent before the first attempt or during a retry delay.
    pub interrupted_attempt: Option<InterruptedAttempt>,
}

/// Terminal assessment of an operation, independent of task state and recovery policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OperationStatus {
    /// The algorithm succeeded.
    Succeeded,
    /// The algorithm returned a partially useful result.
    Degraded,
    /// The algorithm failed normally, including exhausted or non-retryable attempts.
    Failed,
    /// The monitor cancelled local execution.
    Cancelled,
    /// Local execution ended with a caught panic.
    Panicked,
}

/// A normal result or explicit evidence of interrupted execution.
///
/// These payloads define the next event contract. Runtime publication of cancellation
/// and panic reports is introduced separately; the type alone provides no delivery guarantee.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "Keep terminal evidence inline; OperationFinished already boxes this report"
)]
pub enum OperationReport {
    /// The algorithm returned its complete protocol-specific operation report.
    #[non_exhaustive]
    Completed {
        /// Normal evidence, including successful or failed attempts and the final result.
        outcome: ScenarioOutcome,
    },
    /// The monitor cancelled local execution.
    #[non_exhaustive]
    Cancelled {
        /// Why cancellation was requested.
        reason: OperationCancellationReason,
        /// Evidence collected before cancellation completed.
        report: InterruptedOperationReport,
    },
    /// Local execution ended with a caught panic.
    #[non_exhaustive]
    Panicked {
        /// The panic payload rendered as text.
        reason: String,
        /// Evidence collected before the panic.
        report: InterruptedOperationReport,
    },
}

impl OperationReport {
    /// Assesses the result without prescribing continuation, restart, or limit accounting.
    pub const fn status(&self) -> OperationStatus {
        match self {
            Self::Completed { outcome } => match outcome.status() {
                OutcomeStatus::Succeeded => OperationStatus::Succeeded,
                OutcomeStatus::Degraded => OperationStatus::Degraded,
                OutcomeStatus::Failed => OperationStatus::Failed,
            },
            Self::Cancelled { .. } => OperationStatus::Cancelled,
            Self::Panicked { .. } => OperationStatus::Panicked,
        }
    }
}
