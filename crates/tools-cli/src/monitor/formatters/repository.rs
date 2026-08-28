// crates/tools-cli/src/monitor/formatters/repository.rs
use super::format_oids;
use tools_core::monitor::TaskRepository;
use tools_core::polling::PollResult;

pub fn format_repository(repo: &TaskRepository) -> String {
    let mut output = String::new();

    // ============================================================
    // Заголовок
    // ============================================================
    output.push_str("═══════════════════════════════════════════════════════════════\n");
    output.push_str(&format!("📊 Monitor created at: {}\n", repo.created_at()));
    output.push_str("═══════════════════════════════════════════════════════════════\n\n");

    // ============================================================
    // Задачи
    // ============================================================
    for task in repo.tasks_sorted_by_id() {
        let meta = task.meta();
        let data = task.data();

        // Метаданные
        output.push_str(&format!(
            "{} [ID: {}]  Target: {} Created: {}\n",
            meta.name,
            task.id(),
            meta.target,
            //meta.interval,
            task.created_at()
        ));

        // ✅ SUBJECT — список OID, которые запрашиваются
        output.push_str(&format!("{}\n", meta.subject));

        output.push_str("────────────────────────────────────────────────────────────────\n");

        // Метрики
        let m = data.metrics();
        output.push_str(&format!(
            "Requests: {} (✓{} ✗{})  |  Latency: {}ms (avg: {}ms)\n",
            m.total_attempts, m.successful, m.errors, m.current_latency_ms, m.avg_latency_ms
        ));
        output.push_str("────────────────────────────────────────────────────────────────\n");

        // Response
        match data.result() {
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

        // Разделитель между задачами
        output.push_str("································································\n");
        output.push('\n');
    }

    output
}
