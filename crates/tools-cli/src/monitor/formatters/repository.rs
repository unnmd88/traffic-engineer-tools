use crate::monitor::formatters::constants::{LINE_DOTTED_LN, LINE_THIN_LN};

use super::format_oids;
use tools_core::DT_FMT_WITH_MICROSECONDS;
use tools_core::monitor::task::{MonitorSnapshot, TaskView};
use tools_core::monitor::usecase::UseCaseOutput;
use tools_core::polling::Response;

pub fn format_snapshot(snapshot: &MonitorSnapshot) -> String {
    let mut output = String::new();

    for view in &snapshot.tasks {
        output.push_str(&format_task(view));
        output.push_str(LINE_DOTTED_LN);
        output.push('\n');
    }

    output
}

fn format_task(view: &TaskView) -> String {
    let mut output = String::new();

    // Metadata
    output.push_str(&format!(
        "{} [ID: {}]  Target: {}\n",
        view.name, view.id, view.target,
    ));

    let limit = match view.limit {
        0 => "infinity".to_string(),
        _ => format!(
            "{}({} remained)",
            view.limit,
            view.limit.saturating_sub(view.metrics.total_attempts)
        ),
    };

    output.push_str(&format!("Interval: {} Limit: {limit}\n", view.interval.as_secs()));

    output.push_str(LINE_THIN_LN);

    // Metrics
    let latency = if view.metrics.successful > 0 {
        format!(
            "{}ms (min: {}ms max: {}ms)",
            view.metrics.current_latency_ms, view.metrics.min_latency_ms, view.metrics.max_latency_ms
        )
    } else {
        "n/a".to_string()
    };
    output.push_str(&format!(
        "Status: {}\nRequests: {} (✓{} ✗{})  |  Latency: {}\n",
        view.status,
        view.metrics.total_attempts,
        view.metrics.successful,
        view.metrics.errors,
        latency,
    ));

    // History
    if !view.history.is_empty() {
        output.push_str(&format!("History: {}\n", view.history.len()));
        for h in &view.history {
            output.push_str(&format!(
                "{} {}\n",
                match h.result.as_ref() {
                    None => "initial".to_string(),
                    Some(Response::Success {
                        attempts, elapsed, ..
                    }) => format!("success(attempts: {attempts}, {}ms)", elapsed.as_millis()),
                    Some(Response::NoResponse { .. }) => "no response".to_string(),
                },
                h.timestamp.format(DT_FMT_WITH_MICROSECONDS)
            ));
        }
    }

    output.push_str(LINE_THIN_LN);

    // Response
    match view.result.as_ref() {
        Some(Response::Success { payload, .. }) => {
            let UseCaseOutput::SnmpGet(snmp) = payload;
            output.push_str("Snmp-get response:\n");
            output.push_str(&format_oids(&snmp.samples));
            output.push('\n');
        }
        Some(Response::NoResponse { errors, .. }) => {
            output.push_str(&format!("No response: {} attempts\n", errors.len()));
        }
        None => {}
    }

    output
}
