//! Конвертация SCN между форматами: строка ↔ dotted ↔ scn-index ↔ utc-index.
//!
//! Пример: `cargo run --example scn -- str CO101`

use anyhow::{bail, Result};
use tools_core::scn::Scn;

fn parse(from: &str, input: &str) -> Result<Scn> {
    match from {
        "str" => Ok(Scn::from_str(input)?),
        "dotted" => Ok(Scn::from_dotted(input)?),
        "scn-index" => Ok(Scn::from_scn_index(input)?),
        "utc-index" => Ok(Scn::from_utc_index(input)?),
        other => bail!("unknown format: {other}"),
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let [_, from, value] = args.as_slice() else {
        bail!("usage: scn <str|dotted|scn-index|utc-index> <value>");
    };

    let scn = parse(from, value)?;
    println!("scn:       {}", scn.as_str());
    println!("dotted:    {}", scn.to_dotted());
    println!("scn-index: {}", scn.to_scn_index());
    println!("utc-index: {}", scn.to_utc_index());
    Ok(())
}
