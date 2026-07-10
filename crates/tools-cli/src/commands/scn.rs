use anyhow::Result;
use serde::Serialize;
use tools_core::{error::SnmpError, models::Ascii};

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

    pub fn as_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    pub fn as_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self)
    }
}

pub async fn run_from_string(input: String) -> Result<ScnResult> {
    let engine = Ascii::from_str(&input)?;

    Ok(ScnResult {
        input,
        decoded: engine.as_string().to_string(),
        scn: engine.scn().to_string(),
    })
}

pub async fn run_from_scn(input: String) -> Result<ScnResult> {
    let engine = Ascii::from_scn(&input)?;

    Ok(ScnResult {
        input,
        decoded: engine.as_string().to_string(),
        scn: engine.scn().to_string(),
    })
}
