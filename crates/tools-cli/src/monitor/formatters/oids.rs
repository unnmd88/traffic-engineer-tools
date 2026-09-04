// crates/tools-cli/src/monitor/formatters/oids.rs
use tabled::{Table, Tabled, settings::Style};
use tools_core::snmp::SnmpGetSample;

#[derive(Tabled)]
struct OidRow {
    data: String,
    name: String,
    oid: String,
}

pub fn format_oids(samples: &[SnmpGetSample]) -> String {
    let rows: Vec<OidRow> = samples
        .iter()
        .map(|s| OidRow {
            data: format_data(s),
            name: s.oid_name.as_deref().unwrap_or("-").to_string(),
            oid: s.oid.to_string(),
        })
        .collect();

    Table::new(rows).with(Style::rounded()).to_string()
}
fn format_data(sample: &SnmpGetSample) -> String {
    let raw_display = sample.raw_value.to_string();

    match &sample.value {
        Some(v) => format!("{} → {}", raw_display, v),
        None => raw_display,
    }
}
