use tokio::sync::broadcast;

use crate::monitor::{event::MonitorEvent, snapshot::MonitorSnapshot};

#[derive(Debug)]
pub struct MonitorSubscription {
    pub snapshot: MonitorSnapshot,
    pub events: broadcast::Receiver<MonitorEvent>,
}
