// crates/tools-cli/src/monitor/formatters/oids.rs
use tabled::{Table, Tabled, settings::Style};
use tools_core::snmp::SnmpGetSample;

#[derive(Tabled)]
struct OidRow {
    raw: String,
    value: String,
    name: String,
    oid: String,
}

pub fn format_oids(samples: &[SnmpGetSample]) -> String {
    if samples.is_empty() {
        return "  (no data)".to_string();
    }

    let rows: Vec<OidRow> = samples
        .iter()
        .map(|s| {
            OidRow {
                raw: s.raw_value.as_string(),
                value: s
                    .value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or("-".to_string()),
                name: s.oid_name.as_deref().unwrap_or("-").to_string(),
                oid: s.oid.to_string(), // ← полный OID
            }
        })
        .collect();

    Table::new(rows).with(Style::rounded()).to_string()
}
