use anyhow::Result;
use serde::Serialize;
use tools_core::scn::Scn;

#[derive(Debug, Clone, Serialize)]
pub struct ScnResult {
    scn: String,
    index: String,
}

impl std::fmt::Display for ScnResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scn: '{}'\nutc: '{}'", self.scn, self.index)
    }
}

pub fn run_from_string(input: String) -> Result<ScnResult> {
    let scn = Scn::from_str(&input)?;

    Ok(ScnResult {
        scn: scn.as_str().to_string(),
        index: scn.to_utc_index(),
    })
}

pub fn run_from_scn(input: String) -> Result<ScnResult> {
    let scn = Scn::from_utc_index(&input)?;

    Ok(ScnResult {
        scn: scn.as_str().to_string(),
        index: scn.to_utc_index(),
    })
}
