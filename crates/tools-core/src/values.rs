use std::fmt::{self, write};

#[derive(Debug, Clone)]
pub enum Name {
    Stage,
    Detector,
    DetLogic,
    Plan,
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage => write!(f, "stage"),
            Self::Detector => write!(f, "detector"),
            Self::DetLogic => write!(f, "det_logic"),
            Self::Plan => write!(f, "plan"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SnmpRawValue {
    String(String),
    Integer(i32),
    Undigned(u32),
}

#[derive(Debug, Clone)]
pub enum Mode {
    VA,
    FT,
    UTC,
    CLF,
    MAN,
}

#[derive(Debug, Clone)]
pub enum ControllerValue {
    Stage(u8),
    DetCount(u16),
    Mode(Mode),
}
