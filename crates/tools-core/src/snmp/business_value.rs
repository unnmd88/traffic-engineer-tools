use std::fmt::{self, Display, Formatter, write};

use tokio::time::error::Elapsed;

use crate::domain::stage::Stage;

#[derive(Debug, Clone)]
pub enum BusinessValue {
    Stage(Stage),
    StageUg405 { number: Stage, hex: String },
    Integer32(i32),
    Unsigned64(u64),
    Flags { bits: Vec<bool> },
    Text(String),
    SiteId { bytes: Vec<u8> },
}

impl Display for BusinessValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage(s) => write!(f, "{s}"),
            Self::StageUg405 { number, hex } => write!(f, "{number}"),
            Self::Integer32(v) => write!(f, "{v}"),
            Self::Unsigned64(v) => write!(f, "{v}"),
            Self::Flags { bits } => {
                let s: String = bits.iter().map(|v| if *v { '1' } else { '0' }).collect();
                write!(f, "{s}")
            }
            Self::Text(s) => write!(f, "{s}"),
            Self::SiteId { bytes } => write!(f, "{}", String::from_utf8_lossy(&bytes)),
        }
    }
}
