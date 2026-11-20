use crate::monitor::{ApplicationId, TaskSnapshot};

/// An owned snapshot of all tasks and the boundary of the monitor's event stream.
#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    /// The monitor instance whose state was captured.
    pub application_id: ApplicationId,
    /// The sequence number of the next event after this snapshot.
    pub next_seq: u64,
    /// Task snapshots ordered by task identifier.
    pub tasks: Vec<TaskSnapshot>,
}
