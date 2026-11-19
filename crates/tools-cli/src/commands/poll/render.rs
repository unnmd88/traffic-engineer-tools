use tabled::{Table, Tabled, settings::Style};
use tools_core::{
    DT_FMT_WITH_MICROSECONDS,
    monitor::{
        PollSchedule, PreparationStageOutcome, ScenarioExecution, ScenarioOutcome, ScenarioSpec,
        TaskView,
    },
    polling::{AttemptOutcome, PollOutcome, PollReport},
    snmp::SnmpGetSample,
};

pub const LINE_DOUBLE_LN: &str =
    "════════════════════════════════════════════════════════════════════════════════\n";

pub fn format_snapshot(tasks: &[TaskView]) -> String {
    let mut out = String::new();

    for (i, view) in tasks.iter().enumerate() {
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

    let (schedule, attempt) = match &view.scenario {
        ScenarioSpec::SnmpGet(spec) => (spec.schedule(), spec.attempt()),
        ScenarioSpec::SnmpSet(spec) => (spec.schedule(), spec.attempt()),
    };
    let cadence = match schedule {
        PollSchedule::FixedRate { period } => format!("Period: {period:?} (fixed rate)"),
        PollSchedule::FixedDelay { delay } => format!("Delay: {delay:?} (fixed delay)"),
    };
    let timings = format!(
        "{cadence} retries: {} timeout: {:?} retry_delay: {:?}",
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
    if let Some(progress) = &view.progress {
        out.push_str(&format!("Activity: {}\n", progress.message));
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
    if let Some(preparation) = &view.preparation {
        out.push_str(&format!(
            "Preparation (generation {:?}, revision {}):\n",
            preparation.generation, preparation.revision
        ));
        for stage in &preparation.stages {
            let outcome = match &stage.outcome {
                PreparationStageOutcome::Succeeded(data) => format!("{data:?}"),
                PreparationStageOutcome::Failed(error) => error.to_string(),
                PreparationStageOutcome::NotRequired => "not required".to_string(),
            };
            out.push_str(&format!(
                "  {:?}: {}ms, {outcome}\n",
                stage.stage,
                stage.elapsed.as_millis()
            ));
        }
    }
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
            ScenarioOutcome::SnmpGet(report) => {
                let mut out = format_report(report);
                if let PollOutcome::Succeeded(samples) = &report.outcome {
                    out.push_str(&format!("Snmp-get response:\n{}\n", format_oids_table(samples)));
                }
                out
            }
            ScenarioOutcome::SnmpSet(report) => {
                let mut out = format_report(report);
                if matches!(report.outcome, PollOutcome::Succeeded(())) {
                    out.push_str("Snmp-set response: ok\n");
                }
                out
            }
            _ => format!("Scenario outcome: {:?}\n", execution.outcome.status()),
        },
        None => String::new(),
    }
}

fn format_report<T, E: std::fmt::Display>(report: &PollReport<T, E>) -> String {
    let mut out = format!("{}\n", format_response_summary(report));
    for (index, attempt) in report.attempts.iter().enumerate() {
        let outcome = match &attempt.outcome {
            AttemptOutcome::Succeeded => "success".to_string(),
            AttemptOutcome::TimedOut => "timeout".to_string(),
            AttemptOutcome::Failed { error, retryable } => {
                format!("{error} (retryable: {retryable})")
            }
        };
        out.push_str(&format!(
            "  Attempt {}: +{}ms, {}ms, {outcome}\n",
            index + 1,
            attempt.started_after.as_millis(),
            attempt.elapsed.as_millis()
        ));
    }
    out
}

fn format_history(view: &TaskView) -> String {
    let mut out = format!("History: {}\n", view.history.len());
    for h in &view.history {
        let kind = format_execution_summary(&h.result);
        out.push_str(&format!("  {kind} {}\n", h.timestamp.format(DT_FMT_WITH_MICROSECONDS)));
    }
    out
}

fn format_execution_summary(execution: &ScenarioExecution) -> String {
    match &execution.outcome {
        ScenarioOutcome::SnmpGet(response) => format_response_summary(response),
        ScenarioOutcome::SnmpSet(response) => format_response_summary(response),
        _ => format!("{:?}", execution.outcome.status()),
    }
}

fn format_response_summary<T, E>(report: &PollReport<T, E>) -> String {
    let status = match &report.outcome {
        PollOutcome::Succeeded(_) => "success",
        PollOutcome::RetryLimitReached => "retry limit reached",
        PollOutcome::NonRetryableFailure => "non-retryable failure",
    };
    format!(
        "{status}(attempts: {}, {}ms)",
        report.attempt_count(),
        report.elapsed.as_millis()
    )
}

fn push_query(lines: &mut Vec<String>, scenario: &ScenarioSpec) {
    match scenario {
        ScenarioSpec::SnmpGet(spec) => {
            let request = spec.request();
            lines.push("Oids to request:".to_string());
            for item in request.oids() {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{}", item.oid));
            }
        }
        ScenarioSpec::SnmpSet(spec) => {
            let request = spec.request();
            lines.push("Oids to set:".to_string());
            for item in request.sets() {
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
