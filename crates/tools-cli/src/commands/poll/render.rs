use std::collections::BTreeMap;

use tabled::{Table, Tabled, settings::Style};

use tools_core::DT_FMT_WITH_MICROSECONDS;
use tools_core::monitor::probe::{ProbeExecution, ProbeOutcome};
use tools_core::monitor::scenario::Scenario;
use tools_core::monitor::task::{TaskId, TaskView};
use tools_core::polling::Response;
use tools_core::snmp::SnmpGetSample;

pub const LINE_DOUBLE_LN: &str =
    "════════════════════════════════════════════════════════════════════════════════\n";

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
    lines.push(labeled("Revision", &view.revision.to_string()));

    let (interval, attempt) = match &view.scenario {
        Scenario::SnmpGet(spec) => (spec.interval(), spec.attempt()),
        Scenario::SnmpSet(spec) => (spec.interval(), spec.attempt()),
    };
    let timings = format!(
        "Interval: {:?} retries: {} timeout: {:?} retry_delay: {:?}",
        interval,
        attempt.retries(),
        attempt.timeout(),
        attempt.retry_delay()
    );

    lines.push(timings);
    lines.push(labeled("Limit", &format_limit(view)));

    push_query(&mut lines, &view.scenario);

    bordered(&lines)
}

fn format_results(view: &TaskView) -> String {
    let mut out = String::new();

    match &view.health.last_error {
        Some(err) => out.push_str(&format!("Status: {} ({err})\n", view.status)),
        None => out.push_str(&format!("Status: {}\n", view.status)),
    }
    out.push_str(&format!(
        "Executions: {} (success: {} degraded: {} failed: {})  |  Duration: {}\n",
        view.metrics.executions,
        view.metrics.successful,
        view.metrics.degraded,
        view.metrics.failed,
        execution_duration(view),
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
        Some(execution) => match &execution.outcome {
            ProbeOutcome::SnmpGet(Response::Success {
                payload: samples, ..
            }) => {
                format!("Snmp-get response:\n{}\n", format_oids_table(samples))
            }
            ProbeOutcome::SnmpSet(Response::Success { .. }) => {
                "Snmp-set response: ok\n".to_string()
            }
            ProbeOutcome::SnmpGet(Response::NoResponse { attempts, .. })
            | ProbeOutcome::SnmpSet(Response::NoResponse { attempts, .. }) => {
                format!("No response: {attempts} attempts\n")
            }
            _ => format!("Probe outcome: {:?}\n", execution.outcome.status()),
        },
        None => String::new(),
    }
}

fn format_history(view: &TaskView) -> String {
    let mut out = format!("History: {}\n", view.history.len());
    for h in &view.history {
        let kind = format_execution_summary(&h.result);
        out.push_str(&format!("  {kind} {}\n", h.timestamp.format(DT_FMT_WITH_MICROSECONDS)));
    }
    out
}

fn format_execution_summary(execution: &ProbeExecution) -> String {
    match &execution.outcome {
        ProbeOutcome::SnmpGet(response) => format_response_summary(response),
        ProbeOutcome::SnmpSet(response) => format_response_summary(response),
        _ => format!("{:?}", execution.outcome.status()),
    }
}

fn format_response_summary<T>(response: &Response<T>) -> String {
    match response {
        Response::Success {
            attempts, elapsed, ..
        } => {
            format!("success(attempts: {attempts}, {}ms)", elapsed.as_millis())
        }
        Response::NoResponse { attempts, .. } => format!("no response(attempts: {attempts})"),
    }
}

fn push_query(lines: &mut Vec<String>, scenario: &Scenario) {
    match scenario {
        Scenario::SnmpGet(spec) => {
            let request = spec.request();
            lines.push("Oids to request:".to_string());
            for item in &request.oids {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{}", item.oid));
            }
        }
        Scenario::SnmpSet(spec) => {
            let request = spec.request();
            lines.push("Oids to set:".to_string());
            for item in &request.sets {
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
        n => format!("{n} ({} left)", n.saturating_sub(view.metrics.executions)),
    }
}

fn execution_duration(view: &TaskView) -> String {
    if let (Some(min), Some(max)) = (view.metrics.min_duration, view.metrics.max_duration) {
        format!(
            "{}ms (min: {}ms max: {}ms)",
            view.metrics.current_duration.as_millis(),
            min.as_millis(),
            max.as_millis()
        )
    } else {
        "n/a".to_string()
    }
}
