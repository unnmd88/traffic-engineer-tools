use crate::monitor::formatters::constants::{LINE_DOTTED_LN, LINE_THIN_LN};

use super::format_oids;
use chrono::{DateTime, Local};
use tools_core::DT_FMT;
use tools_core::monitor::TaskRepository;
use tools_core::monitor::application::app::ApplicationId;
use tools_core::polling::PollResult;

/*
pub fn create_header(app_id: &ApplicationId, created_at: &DateTime<Local>) -> String {
    let mut output = String::new();

    output.push_str(LINE_DOUBLE_LN);
    output.push_str(&format!(
        "📊 Monitor[ID: {}] created at: {}\n",
        app_id,
        created_at.format(DT_FMT)
    ));
    output.push_str(&format!("{LINE_DOUBLE_LN}\n"));

    output
}
*/

pub fn format_repository(repo: &TaskRepository) -> String {
    let mut output = String::new();

    for task in repo.tasks_sorted_by_id() {
        let meta = task.meta();
        let task_snapshot = task.snapshot();

        // Metadata
        output.push_str(&format!("{} [ID: {}]  Target: {}\n", meta.name, task.id(), meta.target,));

        output.push_str(&format!("{}\n", meta.subject));

        output.push_str(LINE_THIN_LN);

        // Metrics
        let m = task_snapshot.metrics();
        output.push_str(&format!(
            "Status: {}\nRequests: {} (✓{} ✗{})  |  Latency: {}ms (min: {}ms max: {}ms avg: {}ms)\n",
            task_snapshot.poll_status(),
            m.total_attempts,
            m.successful,
            m.errors,
            m.current_latency_ms,
            m.min_latency_ms,
            m.max_latency_ms,
            m.avg_latency_ms
        ));
        output.push_str(LINE_THIN_LN);

        // Response
        match task_snapshot.poll_result() {
            PollResult::SnmpGet(response) => {
                output.push_str("Snmp-get response:\n");
                output.push_str(&format_oids(&response.payload.samples));
                output.push('\n');
            }
            PollResult::NoResponse(errors) => {
                output.push_str(&format!("No response: {} attempts\n", errors.len()));
            }
            _ => {}
        }

        output.push_str(LINE_DOTTED_LN);
        output.push('\n');
    }

    output
}
