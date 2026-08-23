use derive_more::{AsRef, Deref, Display, From, Into};

use crate::error::ParseError;

#[derive(Debug, Clone, Hash, Eq, PartialEq, From, Into, AsRef, Deref, Display)]
pub struct Community(String);

impl Community {
    pub fn parse(value: String) -> Result<Self, ParseError> {
        if value.is_empty() {
            return Err(ParseError::CantBeEmpty {
                name: "community".to_string(),
            });
        }
        if value.len() > 32 {
            return Err(ParseError::InvalidLength {
                message: "community string too long.".to_string(),
                min: 1,
                max: 32,
                provide: value.len(),
            });
        }
        Ok(Self(value))
    }
}
