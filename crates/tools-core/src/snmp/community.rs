use std::str::FromStr;

use derive_more::{AsRef, Deref, Display, Into};

use crate::snmp::ParseError;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Into, AsRef, Deref, Display)]
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

impl TryFrom<String> for Community {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl FromStr for Community {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_conversions_enforce_the_same_byte_length_limits() {
        for value in [
            String::new(),
            "p".to_string(),
            "p".repeat(32),
            "p".repeat(33),
            "я".repeat(17),
        ] {
            let results = [
                Community::parse(value.clone()),
                Community::try_from(value.clone()),
                value.parse::<Community>(),
            ];
            for result in results {
                match value.len() {
                    0 => assert!(matches!(result, Err(ParseError::CantBeEmpty { .. }))),
                    1..=32 => assert_eq!(result.unwrap().as_str(), value),
                    length => assert!(matches!(
                        result,
                        Err(ParseError::InvalidLength { min: 1, max: 32, provide, .. })
                            if provide == length
                    )),
                }
            }
        }
    }

    #[test]
    fn validated_values_can_still_be_consumed_as_strings() {
        let community: Community = "public".parse().unwrap();
        let value: String = community.into();
        assert_eq!(value, "public");
    }
}
