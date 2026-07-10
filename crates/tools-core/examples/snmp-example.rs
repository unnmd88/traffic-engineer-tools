use anyhow::{Context, Result};
use std::net::IpAddr;
use tools_core::models::Ascii;

use tools_core::presentation::Presentable;
use tools_core::{get_timestamp_fmt, presentation};

#[tokio::main]
async fn main() -> Result<()> {
    let scn = "CO4455";

    let ascii1 = Ascii::from_str(scn)?;
    println!("len: {}", ascii1.len());
    println!("decoded: {}", ascii1.as_string());
    println!("codes: {:?}", ascii1.codes());
    println!("codes_delimited: {:?}", ascii1.delimited_codes());
    println!("scn: {:?}", ascii1.scn());

    let from_scn = Ascii::from_scn(&".1.6.67.79.52.48.53.53")?;

    println!("\nfrom scn={}", from_scn.as_string());

    Ok(())
}
