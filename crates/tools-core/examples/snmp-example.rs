use anyhow::{Context, Result};
use std::net::IpAddr;
use tools_core::models::Ascii;

use tools_core::models::snmp::{Sample, SnmpResponse};
use tools_core::presentation::Presentable;
use tools_core::values::{ControllerValue, Name, SnmpRawValue};
use tools_core::{get_timestamp_fmt, presentation};

#[tokio::main]
async fn main() -> Result<()> {
    let sample = Sample {
        oid: "1.3.6.1.4.1.13267.3.2.5.1.1.3.2.1".to_string(),
        name: Some(Name::Stage),
        value: ControllerValue::Stage(8),
        raw: SnmpRawValue::String("test".to_string()),
    };

    println!("\n\n{:?}", sample);

    let response = SnmpResponse {
        target: "192.168.45.15".parse::<IpAddr>().expect("Invalid IpAddr"),
        timestamp: get_timestamp_fmt(),
        payload: vec![sample],
    };
    println!("{:#?}", response);

    let snmp_response = response.to_pretty_string();
    println!("{snmp_response}");

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
