use std::str::FromStr;

use derive_more::{Constructor, Display};

use crate::error::ParseError;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Display, Constructor)]
pub struct Stage(u32);

impl FromStr for Stage {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let num = s.parse::<u32>().map_err(|_| ParseError::Common {
            message: format!("Invalid stage number: '{}'", s),
        })?;

        Ok(Self(num))
    }
}
