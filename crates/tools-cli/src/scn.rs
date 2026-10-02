use anyhow::Result;
use serde::Serialize;
use tools_core::scn::Scn;

#[derive(Debug, Clone, Serialize)]
pub struct ScnResult {
    input: String,
    decoded: String,
    scn: String,
}

impl ScnResult {
    pub fn as_pretty_string(&self) -> String {
        format!("Ввод={:?}\nScn={:?}\nЗакодированный Scn={}", self.input, self.decoded, self.scn)
    }
}

pub fn run_from_string(input: String) -> Result<ScnResult> {
    let engine = Scn::from_str(&input)?;

    Ok(ScnResult {
        input,
        decoded: engine.as_str().to_string(),
        scn: engine.to_scn_index(),
    })
}

pub fn run_from_scn(input: String) -> Result<ScnResult> {
    let engine = Scn::from_scn_index(&input)?;

    Ok(ScnResult {
        input,
        decoded: engine.as_str().to_string(),
        scn: engine.to_scn_index(),
    })
}
