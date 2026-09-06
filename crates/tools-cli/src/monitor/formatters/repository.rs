use crate::monitor::formatters::constants::{LINE_DOTTED_LN, LINE_THIN_LN};

use super::format_oids;
use tools_core::DT_FMT_WITH_MICROSECONDS;
use tools_core::monitor::task::TaskRepository;
use tools_core::monitor::usecase::UseCaseOutput;
use tools_core::polling::Response;

pub fn format_repository(repo: &TaskRepository) -> String {
    let mut output = String::new();

    for task in repo.tasks_sorted_by_id() {
        let task_snapshot = task.snapshot();
        let poll_config = task.poll_config();
        let metrics = task_snapshot.metrics();
        let history = task.history();

        // Metadata
        output.push_str(&format!(
            "{} [ID: {}]  Target: {}\n",
            task.name(),
            task.id(),
            task.query().target(),
        ));

        let limit = match poll_config.limit() {
            0 => "infinity".to_string(),
            _ => format!(
                "{}({} remained)",
                poll_config.limit(),
                poll_config.limit().saturating_sub(metrics.total_attempts)
            ),
        };

        output.push_str(&format!("Interval: {} Limit: {limit}\n", poll_config.interval().as_secs()));

        output.push_str(LINE_THIN_LN);

        // Metrics
        let latency = if metrics.successful > 0 {
            format!(
                "{}ms (min: {}ms max: {}ms)",
                metrics.current_latency_ms, metrics.min_latency_ms, metrics.max_latency_ms
            )
        } else {
            "n/a".to_string()
        };
        output.push_str(&format!(
            "Status: {}\nRequests: {} (✓{} ✗{})  |  Latency: {}\n",
            task_snapshot.poll_status(),
            metrics.total_attempts,
            metrics.successful,
            metrics.errors,
            latency,
        ));
        if !history.is_empty() {
            output.push_str(&format!("History: {}\n", history.len()));
            for h in history.iter() {
                output.push_str(&format!(
                    "{} {}\n",
                    match h.snapshot.poll_result() {
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
        match task_snapshot.poll_result() {
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

        output.push_str(LINE_DOTTED_LN);
        output.push('\n');
    }

    output
}
