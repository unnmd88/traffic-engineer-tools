//! Canonical consumer example for the Monitor.
//!
//! Shows the full lifecycle from a consumer's point of view:
//! build a task, subscribe before start, spawn, read events, take a snapshot, shut down.

use std::time::Duration;

use tools_core::{
    monitor::{AppConfig, Application, EventData, PollSchedule, ScenarioSpec, TaskConfig},
    polling::AttemptConfig,
    snmp::{RawSnmpOidItem, SnmpGetSpec},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Describe one task: a repeating SNMP GET of a single OID.
    let request = SnmpGetSpec::from_raw(
        "127.0.0.1".to_string(),
        161,
        "public".to_string(),
        None,
        vec![RawSnmpOidItem {
            name: Some("sysDescr".to_string()),
            oid: "1.3.6.1.2.1.1.1.0".to_string(),
        }],
    )?;
    let scenario = ScenarioSpec::snmp_get(
        request,
        PollSchedule::fixed_delay(Duration::from_secs(1))?,
        AttemptConfig::try_new(Duration::from_millis(500), 2, Duration::from_millis(100))?,
    )?;
    // `limit` counts complete executions; Success and NoResponse both count.
    let task = TaskConfig::try_new("sysDescr".to_string(), scenario, 3, 3)?;

    // 2. Assemble the application and subscribe BEFORE start so no event is missed.
    let app = Application::new(AppConfig::default(), vec![task]);
    let mut sub = app.subscribe_with_snapshot();
    println!("snapshot before start: {} task(s)", sub.snapshot.tasks.len());

    // 3. Start and take a handle for commands and snapshots.
    let running = app.spawn();
    let handle = running.handle();

    // 4. Read events until the task completes.
    while let Ok(event) = sub.events.recv().await {
        println!("seq {}: {:?}", event.metadata.seq, event.data);
        if matches!(event.data, EventData::TaskCompleted) {
            break;
        }
    }

    // 5. Final snapshot: status and result of the task.
    let snapshot = handle.get_snapshot().await?;
    for view in &snapshot.tasks {
        println!(
            "task '{}': status={:?}, has_result={}",
            view.name,
            view.status,
            view.result.is_some()
        );
    }

    // 6. Graceful shutdown waits for the orchestrator, scenarios, and timers.
    running.shutdown().await?;
    Ok(())
}
