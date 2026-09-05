use derive_more::{Constructor, Display};

#[derive(Clone, Debug, Copy, Display)]
pub enum Protocol {
    Snmp,
    Http,
    Modbus,
}

#[derive(Clone, Debug, Copy, Display)]
pub enum TypeQuery {
    SnmpGet,
}

#[derive(Clone, Debug, Copy, Display)]
pub enum PollStatus {
    Idle,
    Active,
    Paused,
    RatedLimit,
}

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct TaskId(pub u64);
