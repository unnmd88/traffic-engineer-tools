use crate::presentation::Presentable;

pub struct MonitorUpdate {
    monitor_id: u8,
    data: Box<dyn Presentable>,
}
