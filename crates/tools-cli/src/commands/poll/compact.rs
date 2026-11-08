use chrono::{DateTime, Local};
use tools_core::{
    monitor::{OutcomeStatus, ScenarioOutcome, ScenarioSpec, TaskStatus, TaskView},
    polling::{PollErrorContext, Response},
    snmp::{
        SnmpGetSample,
        business_value::BusinessValue,
        group_state_potok_ug405::{GroupStateDefinition, GroupStatePotokUg405},
        value::SnmpValueType,
    },
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const GROUP_CELL_WIDTH: usize = 18;

pub fn format_snapshot(
    tasks: &[TaskView],
    started_at: DateTime<Local>,
    now: DateTime<Local>,
    width: usize,
) -> String {
    let width = width.max(1);
    let mut out = String::new();
    let uptime = (now - started_at).num_seconds().max(0);
    let active = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Active)
        .count();
    let completed = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .count();
    push_line(
        &mut out,
        &format!(
            "POLL | Uptime: {:02}:{:02}:{:02} | Tasks: {} | Active: {active} | Completed: {completed}",
            uptime / 3600,
            uptime / 60 % 60,
            uptime % 60,
            tasks.len(),
        ),
        width,
    );
    let separator = "-".repeat(width);
    out.push_str(&separator);
    out.push('\n');
    if tasks.is_empty() {
        push_line(&mut out, "No tasks", width);
    }
    for task in tasks {
        format_task(&mut out, task, now, width);
        out.push_str(&separator);
        out.push('\n');
    }
    push_line(&mut out, "Ctrl+C: exit", width);
    out
}

fn format_task(out: &mut String, task: &TaskView, now: DateTime<Local>, width: usize) {
    let (scenario, profile) = match &task.scenario {
        ScenarioSpec::SnmpGet(spec) => ("SNMP GET", spec.request().profile()),
        ScenarioSpec::SnmpSet(spec) => ("SNMP SET", spec.request().profile()),
    };
    push_line(
        out,
        &format!("[{}] {} | {} | {scenario}", task.id, task.name, task.target),
        width,
    );
    let profile = profile.map_or_else(|| "none".to_string(), |profile| profile.to_string());
    push_line(out, &format!("State: {} | Profile: {profile}", task.status), width);
    if let Some(error) = &task.health.last_error {
        push_line(out, &format!("Error: {error}"), width);
    }
    let runs = match task.limit {
        0 => task.metrics.executions.to_string(),
        limit => format!("{}/{limit}", task.metrics.executions),
    };
    push_line(
        out,
        &format!(
            "Runs: {runs} | Success: {} | Degraded: {} | Failed: {} | Failed in a row: {}",
            task.metrics.successful,
            task.metrics.degraded,
            task.metrics.failed,
            task.health.consecutive_failures,
        ),
        width,
    );
    let last = if task.metrics.executions == 0 {
        "n/a".to_string()
    } else {
        format!("{}ms", task.metrics.current_duration.as_millis())
    };
    let min = task
        .metrics
        .min_duration
        .map_or_else(|| "n/a".to_string(), |duration| format!("{}ms", duration.as_millis()));
    let max = task
        .metrics
        .max_duration
        .map_or_else(|| "n/a".to_string(), |duration| format!("{}ms", duration.as_millis()));
    push_line(out, &format!("Duration: last {last} | min {min} | max {max}"), width);
    let Some(execution) = &task.result else {
        push_line(out, "Last result: No data", width);
        return;
    };
    let status = match execution.outcome.status() {
        OutcomeStatus::Succeeded => "OK",
        OutcomeStatus::Degraded => "DEGRADED",
        OutcomeStatus::Failed => "FAILED",
    };
    let finished_at = execution.finished_at.with_timezone(&Local);
    let age_ms = (now - finished_at).num_milliseconds().max(0);
    push_line(
        out,
        &format!(
            "Last result: {status} | {} | Age: {}.{}s",
            finished_at.format("%H:%M:%S%.3f"),
            age_ms / 1000,
            age_ms % 1000 / 100,
        ),
        width,
    );
    match &execution.outcome {
        ScenarioOutcome::SnmpGet(response) => {
            format_response(out, response, width);
            if let Response::Success {
                payload: samples, ..
            } = response
            {
                format_samples(out, samples, width);
            }
        }
        ScenarioOutcome::SnmpSet(response) => {
            format_response(out, response, width);
            if matches!(response, Response::Success { .. }) {
                push_line(out, "SET acknowledged", width);
            }
        }
        _ => {}
    }
}

fn format_response<T>(out: &mut String, response: &Response<T>, width: usize) {
    let (attempts, errors) = match response {
        Response::Success {
            attempts, errors, ..
        } => {
            push_line(out, &format!("Reply received | Attempts: {attempts}"), width);
            (*attempts, errors)
        }
        Response::NoResponse {
            attempts, errors, ..
        } => {
            push_line(out, &format!("No response | Attempts: {attempts}"), width);
            (*attempts, errors)
        }
    };
    if let Some(PollErrorContext {
        attempt, message, ..
    }) = errors.last()
    {
        push_line(out, &format!("Last attempt error ({attempt}/{attempts}): {message}"), width);
    }
}

fn format_samples(out: &mut String, samples: &[SnmpGetSample], width: usize) {
    if samples.is_empty() {
        push_line(out, "No values returned", width);
        return;
    }
    let name_width = samples
        .iter()
        .filter(|sample| !matches!(sample.value, Some(BusinessValue::GroupStatePotokUg405 { .. })))
        .map(|sample| sample_name(sample).width())
        .max()
        .unwrap_or(0)
        .min(width / 2);
    out.push('\n');
    for sample in samples {
        match &sample.value {
            Some(BusinessValue::GroupStatePotokUg405 { groups }) => {
                format_groups(out, sample, groups, width);
            }
            value => {
                let name = sample_name(sample);
                let padding = " ".repeat(name_width.saturating_sub(name.width()));
                let value = value
                    .as_ref()
                    .map_or_else(|| "-".to_string(), ToString::to_string);
                push_line(
                    out,
                    &format!(
                        "  {name}{padding}  {value} (raw: {}, {})",
                        sample.raw_value.as_string(),
                        SnmpValueType::from(&sample.raw_value),
                    ),
                    width,
                );
            }
        }
    }
}

fn sample_name(sample: &SnmpGetSample) -> String {
    sample
        .oid_name
        .clone()
        .unwrap_or_else(|| sample.oid.to_string())
}

fn format_groups(
    out: &mut String,
    sample: &SnmpGetSample,
    groups: &[GroupStatePotokUg405],
    width: usize,
) {
    out.push('\n');
    push_line(
        out,
        &format!(
            "  {}: {} groups (raw: {}; hex bytes in cells)",
            sample_name(sample),
            groups.len(),
            SnmpValueType::from(&sample.raw_value),
        ),
        width,
    );
    push_line(out, "    Group: main/arrow (raw byte)", width);
    let columns = (width.saturating_sub(4) / GROUP_CELL_WIDTH).max(1);
    let mut sorted: Vec<_> = groups.iter().collect();
    sorted.sort_by_key(|group| group.num_group());
    for row in sorted.chunks(columns) {
        let mut line = String::from("    ");
        for (index, group) in row.iter().enumerate() {
            let (main, arrow) = group.state().map_or(("?", "?"), |state| {
                let (main, arrow) = state.split_state();
                (short_signal(main), short_signal(arrow))
            });
            let cell =
                format!("{:02}: {main}/{arrow} ({:02X})", group.num_group(), group.source_code());
            line.push_str(&cell);
            if index + 1 < row.len() {
                line.push_str(&" ".repeat(GROUP_CELL_WIDTH.saturating_sub(cell.len())));
            }
        }
        push_line(out, &line, width);
    }
    push_line(out, "    R red | Y amber | G green | RY red+amber | F flashing", width);
    push_line(out, "    - off | ? unknown", width);
    out.push('\n');
}

fn short_signal(signal: &str) -> &'static str {
    match signal {
        GroupStateDefinition::OFF => "-",
        GroupStateDefinition::RED => "R",
        GroupStateDefinition::RED_AMBER => "RY",
        GroupStateDefinition::GREEN => "G",
        GroupStateDefinition::GREEN_FLASH => "GF",
        GroupStateDefinition::AMBER => "Y",
        GroupStateDefinition::AMBER_FLASH => "YF",
        _ => "?",
    }
}

fn push_line(out: &mut String, text: &str, width: usize) {
    let mut escaped = String::new();
    for character in text.chars() {
        if character.is_control() && character != '\n' {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    for mut line in escaped.split('\n') {
        let indent_width = line
            .bytes()
            .take_while(|byte| *byte == b' ')
            .count()
            .min(width.saturating_sub(1));
        let indent = " ".repeat(indent_width);
        let content_width = width - indent_width;
        line = line.trim_start_matches(' ');
        while line.width() > content_width {
            let mut column = 0;
            let mut end = 0;
            let mut last_space = None;
            for (index, character) in line.char_indices() {
                let character_width = character.width().unwrap_or(0);
                if column + character_width > content_width && end > 0 {
                    break;
                }
                column += character_width;
                end = index + character.len_utf8();
                if character == ' ' && index > 0 {
                    last_space = Some(index);
                }
            }
            let (head, tail) = line.split_at(last_space.unwrap_or(end));
            out.push_str(&indent);
            out.push_str(head.trim_end());
            out.push('\n');
            line = tail.trim_start_matches(' ');
        }
        out.push_str(&indent);
        out.push_str(line);
        out.push('\n');
    }
}
