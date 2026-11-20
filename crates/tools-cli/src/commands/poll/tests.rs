use std::time::Duration;

use chrono::Local;
use tools_core::{
    monitor::{AppConfig, Application, PollSchedule, ScenarioSpec, TaskConfig},
    polling::AttemptConfig,
    snmp::{SnmpGetItem, SnmpGetSpec, SnmpOid, SnmpProfile, SnmpSetItem, SnmpSetSpec, SnmpValue},
};

use super::{compact, render};

#[test]
fn both_renderers_display_configuration_from_task_descriptions() {
    let attempt =
        AttemptConfig::try_new(Duration::from_millis(750), 2, Duration::from_millis(100)).unwrap();
    let schedule = PollSchedule::fixed_delay(Duration::from_secs(2)).unwrap();
    let get = SnmpGetSpec::new(
        "[2001:db8::7]:1161".parse().unwrap(),
        "get-secret-sentinel".parse().unwrap(),
        Some(SnmpProfile::PotokUg405),
        vec![SnmpGetItem {
            name: Some("phase-read".to_string()),
            oid: SnmpOid::parse("1.3.6.1.4.1.999.123.0").unwrap(),
            parser: None,
        }],
    );
    let set = SnmpSetSpec::try_new(
        "127.0.0.2:1162".parse().unwrap(),
        Some("set-read-secret-sentinel".parse().unwrap()),
        "set-write-secret-sentinel".parse().unwrap(),
        Some(SnmpProfile::PotokUg405),
        vec![SnmpSetItem {
            name: Some("phase-write".to_string()),
            oid: SnmpOid::parse("1.3.6.1.4.1.999.124.0").unwrap(),
            value: SnmpValue::Integer(7),
        }],
    )
    .unwrap();
    let configs = vec![
        TaskConfig::try_new(
            "read phase".to_string(),
            ScenarioSpec::snmp_get(get, schedule, attempt).unwrap(),
            10,
            4,
        )
        .unwrap(),
        TaskConfig::try_new(
            "write phase".to_string(),
            ScenarioSpec::snmp_set(set, schedule, attempt).unwrap(),
            20,
            5,
        )
        .unwrap(),
    ];
    let application = Application::new(AppConfig::default(), configs);
    let snapshot = application.subscribe_with_snapshot().snapshot;
    let classic = render::format_snapshot(&snapshot.tasks);
    let now = Local::now();
    let compact = compact::format_snapshot(&snapshot.tasks, now, now, 160);

    for output in [&classic, &compact] {
        for expected in [
            "read phase",
            "write phase",
            "[2001:db8::7]:1161",
            "127.0.0.2:1162",
        ] {
            assert!(output.contains(expected), "missing {expected} in {output}");
        }
        assert!(!output.contains("secret-sentinel"));
    }
    for expected in [
        "Revision:",
        "Delay: 2s (fixed delay)",
        "retries: 2 timeout: 750ms retry_delay: 100ms",
        "phase-read",
        "1.3.6.1.4.1.999.123.0",
        "phase-write",
        "1.3.6.1.4.1.999.124.0",
        "= 7",
        "10 (10 left)",
        "20 (20 left)",
    ] {
        assert!(classic.contains(expected), "missing {expected} in {classic}");
    }
    for expected in [
        "SNMP GET",
        "SNMP SET",
        "Runs: 0/10",
        "Runs: 0/20",
        &format!("Profile: {}", SnmpProfile::PotokUg405),
    ] {
        assert!(compact.contains(expected), "missing {expected} in {compact}");
    }
}
