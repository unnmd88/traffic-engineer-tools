use tokio::sync::broadcast;

use crate::monitor::{event::MonitorEvent, task::MonitorSnapshot};

#[derive(Debug)]
pub struct MonitorSubscription {
    pub snapshot: MonitorSnapshot,
    pub events: broadcast::Receiver<MonitorEvent>,
}
