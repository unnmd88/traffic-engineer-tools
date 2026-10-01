use std::collections::BTreeMap;

use tabled::{Table, Tabled, settings::Style};

use tools_core::DT_FMT_WITH_MICROSECONDS;
use tools_core::monitor::adapter::{AdapterOutput, Query};
use tools_core::monitor::task::{TaskId, TaskView};
use tools_core::polling::Response;
use tools_core::snmp::SnmpGetSample;

pub fn format_snapshot(tasks: &BTreeMap<TaskId, TaskView>) -> String {
    let mut out = String::new();

    for (i, view) in tasks.values().enumerate() {
        if i > 0 {
            out.push('\n'); // граница между задачами
        }
        out.push_str(&format_task(view));
    }

    out
}

fn format_task(view: &TaskView) -> String {
    let mut out = format_card(view);
    out.push_str("\n\n");
    out.push_str(&format_results(view));
    out
}

fn format_card(view: &TaskView) -> String {
    let mut lines: Vec<String> = Vec::new();

    lines.push(labeled("Name", &view.name));
    lines.push(labeled("Target", &view.target));

    lines.push(labeled("Interval", &format!("{}s", view.interval.as_secs())));
    lines.push(labeled("Limit", &format_limit(view)));

    push_query(&mut lines, &view.query);

    bordered(&lines)
}

/// Результаты: динамичная часть (каждый опрос). Статус первым — его ищешь взглядом.
fn format_results(view: &TaskView) -> String {
    let mut out = String::new();

    out.push_str(&format!("Status: {}\n", view.status));
    out.push_str(&format!(
        "Requests: {} (success: {} failed: {})  |  Latency: {}\n",
        view.metrics.total_attempts,
        view.metrics.successful,
        view.metrics.errors,
        latency(view),
    ));
    out.push('\n');
    out.push_str(&format_response(view));

    if !view.history.is_empty() {
        out.push('\n');
        out.push_str(&format_history(view));
    }

    out
}

fn format_response(view: &TaskView) -> String {
    match view.result.as_ref() {
        Some(Response::Success { payload, .. }) => match payload {
            AdapterOutput::SnmpGet(samples) => {
                format!("Snmp-get response:\n{}\n", format_oids_table(samples))
            }
            AdapterOutput::SnmpSet(()) => "Snmp-set response: ok\n".to_string(),
        },
        Some(Response::NoResponse { errors, .. }) => {
            format!("No response: {} attempts\n", errors.len())
        }
        None => String::new(),
    }
}

fn format_history(view: &TaskView) -> String {
    let mut out = format!("History: {}\n", view.history.len());
    for h in &view.history {
        let kind = match h.result.as_ref() {
            None => "initial".to_string(),
            Some(Response::Success {
                attempts, elapsed, ..
            }) => format!("success(attempts: {attempts}, {}ms)", elapsed.as_millis()),
            Some(Response::NoResponse { .. }) => "no response".to_string(),
        };
        out.push_str(&format!("  {kind} {}\n", h.timestamp.format(DT_FMT_WITH_MICROSECONDS)));
    }
    out
}

fn push_query(lines: &mut Vec<String>, q: &Query) {
    match q {
        Query::SnmpGet(q) => {
            lines.push("Oids to request:".to_string());
            for item in &q.oids {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{}", item.oid));
            }
        }
        Query::SnmpSet(q) => {
            lines.push("Oids to set:".to_string());
            for item in &q.sets {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{:<32} = {}", item.oid, item.value));
            }
        }
    }
}

#[derive(Tabled)]
struct OidRow {
    data: String,
    name: String,
    oid: String,
}

fn format_oids_table(samples: &[SnmpGetSample]) -> String {
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
    let raw = sample.raw_value.to_string();
    match &sample.value {
        Some(v) => format!("{raw} → {v}"),
        None => raw,
    }
}

fn labeled(label: &str, value: &str) -> String {
    format!("{:<10}{}", format!("{label}:"), value)
}

/// Оборачивает строки в скруглённую рамку (rounded).
fn bordered(lines: &[String]) -> String {
    let width = lines
        .iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0)
        .max(1);

    let mut out = format!("╭{}╮\n", "─".repeat(width + 2));
    for line in lines {
        let pad = width - line.chars().count();
        out.push_str(&format!("│ {}{} │\n", line, " ".repeat(pad)));
    }
    out.push_str(&format!("╰{}╯", "─".repeat(width + 2)));
    out
}

fn format_limit(view: &TaskView) -> String {
    match view.limit {
        0 => "no limit".to_string(),
        n => format!("{n} ({} left)", n.saturating_sub(view.metrics.total_attempts)),
    }
}

fn latency(view: &TaskView) -> String {
    if view.metrics.successful > 0 {
        format!(
            "{}ms (min: {}ms max: {}ms)",
            view.metrics.current_latency_ms,
            view.metrics.min_latency_ms,
            view.metrics.max_latency_ms
        )
    } else {
        "n/a".to_string()
    }
}
