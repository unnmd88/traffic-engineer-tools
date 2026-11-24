use tabled::{Table, Tabled, settings::Style};
use tools_core::{
    DT_FMT_WITH_MICROSECONDS,
    monitor::{
        PollSchedule, RunLimit, ScenarioDescription, ScenarioExecution, ScenarioOutcome,
        TaskSnapshot,
    },
    polling::{AttemptOutcome, PollDetails, PollOutcome},
    snmp::SnmpGetSample,
};

pub const LINE_DOUBLE_LN: &str =
    "════════════════════════════════════════════════════════════════════════════════\n";

pub fn format_snapshot(tasks: &[TaskSnapshot]) -> String {
    let mut out = String::new();

    for (i, view) in tasks.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format_task(view));
    }

    out
}

fn format_task(view: &TaskSnapshot) -> String {
    let mut out = format_card(view);
    out.push_str("\n\n");
    out.push_str(&format_results(view));
    out
}

fn format_card(view: &TaskSnapshot) -> String {
    let mut lines: Vec<String> = Vec::new();
    let configuration = &view.configuration;
    let description = &configuration.description;
    let settings = match &description.scenario {
        ScenarioDescription::SnmpGet {
            target,
            schedule,
            attempt,
            ..
        }
        | ScenarioDescription::SnmpSet {
            target,
            schedule,
            attempt,
            ..
        } => Some((target, schedule, attempt)),
        _ => None,
    };

    lines.push(labeled("Name", &description.name));
    if let Some((target, _, _)) = settings {
        lines.push(labeled("Target", &target.to_string()));
    }
    lines.push(labeled("Revision", &configuration.revision.to_string()));

    if let Some((_, schedule, attempt)) = settings {
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
    }
    lines.push(labeled("Limit", &format_limit(view)));

    push_query(&mut lines, &description.scenario);

    bordered(&lines)
}

fn format_results(view: &TaskSnapshot) -> String {
    let mut out = String::new();

    match &view.last_failure {
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
    out.push_str(&format_response(view));

    if !view.history.is_empty() {
        out.push('\n');
        out.push_str(&format_history(view));
    }

    out
}

fn format_response(view: &TaskSnapshot) -> String {
    match view.result.as_ref() {
        Some(execution) => match execution.report.detail() {
            Some(ScenarioOutcome::SnmpGet(report)) => {
                let mut out = format_report(report, execution.report.elapsed);
                if let PollOutcome::Succeeded(samples) = &report.outcome {
                    out.push_str(&format!("Snmp-get response:\n{}\n", format_oids_table(samples)));
                }
                out
            }
            Some(ScenarioOutcome::SnmpSet(report)) => {
                let mut out = format_report(report, execution.report.elapsed);
                if matches!(report.outcome, PollOutcome::Succeeded(())) {
                    out.push_str("Snmp-set response: ok\n");
                }
                out
            }
            _ => format!("Scenario outcome: {:?}\n", execution.report.status()),
        },
        None => String::new(),
    }
}

fn format_report<T, E: std::fmt::Display>(
    report: &PollDetails<T, E>,
    elapsed: std::time::Duration,
) -> String {
    let mut out = format!("{}\n", format_response_summary(report, elapsed));
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

fn format_history(view: &TaskSnapshot) -> String {
    let mut out = format!("History: {}\n", view.history.len());
    for execution in &view.history {
        let kind = format_execution_summary(execution);
        out.push_str(&format!(
            "  {kind} {}\n",
            execution.finished_at().format(DT_FMT_WITH_MICROSECONDS)
        ));
    }
    out
}

fn format_execution_summary(execution: &ScenarioExecution) -> String {
    match execution.report.detail() {
        Some(ScenarioOutcome::SnmpGet(response)) => {
            format_response_summary(response, execution.report.elapsed)
        }
        Some(ScenarioOutcome::SnmpSet(response)) => {
            format_response_summary(response, execution.report.elapsed)
        }
        _ => format!("{:?}", execution.report.status()),
    }
}

fn format_response_summary<T, E>(
    report: &PollDetails<T, E>,
    elapsed: std::time::Duration,
) -> String {
    let status = match &report.outcome {
        PollOutcome::Succeeded(_) => "success",
        PollOutcome::RetryLimitReached => "retry limit reached",
        PollOutcome::NonRetryableFailure => "non-retryable failure",
    };
    format!("{status}(attempts: {}, {}ms)", report.attempt_count(), elapsed.as_millis())
}

fn push_query(lines: &mut Vec<String>, scenario: &ScenarioDescription) {
    match scenario {
        ScenarioDescription::SnmpGet { oids, .. } => {
            lines.push("Oids to request:".to_string());
            for item in oids {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{}", item.oid));
            }
        }
        ScenarioDescription::SnmpSet { sets, .. } => {
            lines.push("Oids to set:".to_string());
            for item in sets {
                let name = item.name.as_deref().unwrap_or("-");
                lines.push(format!("  {name:<16}{:<32} = {}", item.oid, item.value));
            }
        }
        _ => {}
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

fn format_limit(view: &TaskSnapshot) -> String {
    match view.run_limit {
        Some(RunLimit::Runs(limit)) => {
            format!("{limit} ({} left)", limit.get().saturating_sub(view.completed_runs))
        }
        Some(RunLimit::Unlimited) => "no limit".to_string(),
        None => "n/a".to_string(),
    }
}

fn execution_duration(view: &TaskSnapshot) -> String {
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
