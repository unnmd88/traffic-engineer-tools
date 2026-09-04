use crate::monitor::formatters::constants::{LINE_DOTTED_LN, LINE_THIN_LN};

use super::format_oids;
use tools_core::DT_FMT_WITH_MICROSECONDS;
use tools_core::monitor::TaskRepository;
use tools_core::monitor::application::UseCaseOutput;
use tools_core::polling::PollResult;

pub fn format_repository(repo: &TaskRepository) -> String {
    let mut output = String::new();

    for task in repo.tasks_sorted_by_id() {
        let meta = task.meta();
        let task_snapshot = task.snapshot();
        let poll_config = task.poll_config();
        let m = task_snapshot.metrics();
        let history = task.history();

        // Metadata
        output.push_str(&format!("{} [ID: {}]  Target: {}\n", meta.name, task.id(), meta.target,));

        output.push_str(&format!("{}\n", meta.subject));

        let limit = match poll_config.limit {
            0 => "infinity",
            _ => {
                &format!("{}({} remained)", poll_config.limit, poll_config.limit - m.total_attempts)
            }
        };

        output.push_str(&format!("Interval: {} Limit: {limit}\n", poll_config.interval.as_secs()));

        output.push_str(LINE_THIN_LN);

        // Metrics
        output.push_str(&format!(
            "Status: {}\nRequests: {} (✓{} ✗{})  |  Latency: {}ms (min: {}ms max: {}ms)\n",
            task_snapshot.poll_status(),
            m.total_attempts,
            m.successful,
            m.errors,
            m.current_latency_ms,
            m.min_latency_ms,
            m.max_latency_ms,
        ));
        if !history.is_empty() {
            output.push_str(&format!("History: {}\n", history.len()));
            for h in history.iter() {
                output.push_str(&format!(
                    "{} {}\n",
                    match h.snapshot.poll_result() {
                        PollResult::Initial => "initial".to_string(),
                        PollResult::Success(r) => format!(
                            "success(attempts: {}, {}ms)",
                            r.attempts,
                            r.elapsed.as_millis()
                        ),
                        PollResult::NoResponse(_) => "no response".to_string(),
                        PollResult::Fail { message } => format!("fail: {message}"),
                    },
                    h.timestamp.format(DT_FMT_WITH_MICROSECONDS)
                ));
            }
        }

        output.push_str(LINE_THIN_LN);

        // Response
        match task_snapshot.poll_result() {
            PollResult::Success(response) => {
                if let UseCaseOutput::SnmpGet(snmp) = &response.payload {
                    output.push_str("Snmp-get response:\n");
                    output.push_str(&format_oids(&snmp.samples));
                    output.push('\n');
                }
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
