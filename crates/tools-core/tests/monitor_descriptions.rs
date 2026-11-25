use std::time::Duration;

use serde_json::{Value, json};
use tools_core::{
    monitor::{
        AppConfig, Application, EventData, InitialTask, PollSchedule, RunLimit,
        ScenarioDescription, ScenarioSpec, SpecRevision, TaskConfig, TaskConfigurationSnapshot,
        TaskDescription,
    },
    polling::AttemptConfig,
    snmp::{
        BusinessValue, RawSnmpOidItem, SnmpGetItem, SnmpGetSpec, SnmpOid, SnmpProfile, SnmpSetItem,
        SnmpSetSpec, SnmpValue,
    },
};

const GET_COMMUNITY: &str = "get-secret-sentinel";
const SET_READ_COMMUNITY: &str = "set-read-secret-sentinel";
const SET_WRITE_COMMUNITY: &str = "set-write-secret-sentinel";

fn get_task(name: &str) -> TaskConfig {
    let request = SnmpGetSpec::from_raw(
        "2001:db8::7".to_string(),
        1161,
        GET_COMMUNITY.to_string(),
        Some("potok_ug405".to_string()),
        vec![RawSnmpOidItem {
            name: Some("phase".to_string()),
            oid: "stage".to_string(),
        }],
    )
    .unwrap();
    let scenario = ScenarioSpec::snmp_get(
        request,
        PollSchedule::fixed_delay(Duration::from_secs(2)).unwrap(),
        AttemptConfig::try_new(Duration::from_millis(750), 2, Duration::from_millis(100)).unwrap(),
    )
    .unwrap();
    TaskConfig::try_new(name.to_string(), scenario, 4).unwrap()
}

fn set_task(name: &str) -> TaskConfig {
    let request = SnmpSetSpec::try_new(
        "127.0.0.2:1162".parse().unwrap(),
        Some(SET_READ_COMMUNITY.parse().unwrap()),
        SET_WRITE_COMMUNITY.parse().unwrap(),
        Some(SnmpProfile::PotokUg405),
        vec![SnmpSetItem {
            name: Some("binary".to_string()),
            oid: SnmpOid::parse("1.3.6.1.2.1.1.5.0").unwrap(),
            value: SnmpValue::OctetString(vec![0, 128, 255]),
        }],
    )
    .unwrap();
    let scenario = ScenarioSpec::snmp_set(
        request,
        PollSchedule::fixed_rate(Duration::from_millis(250)).unwrap(),
        AttemptConfig::try_new(Duration::from_millis(50), 1, Duration::ZERO).unwrap(),
    )
    .unwrap();
    TaskConfig::try_new(name.to_string(), scenario, 5).unwrap()
}

fn assert_no_credentials(record: &Value) {
    let text = serde_json::to_string(record).unwrap();
    for secret in [GET_COMMUNITY, SET_READ_COMMUNITY, SET_WRITE_COMMUNITY] {
        assert!(!text.contains(secret));
    }
    assert!(!text.contains("community"));
}

#[test]
fn get_description_retains_normalized_oid_ipv6_target_profile_and_execution_settings() {
    let config = get_task("read phase");
    let description = TaskDescription::from(&config);
    let record = serde_json::to_value(&description).unwrap();
    let ScenarioSpec::SnmpGet(spec) = config.scenario() else {
        panic!("expected GET configuration");
    };
    assert_eq!(
        record,
        json!({
            "name": "read phase",
            "deep_history": 4,
            "scenario": { "SnmpGet": {
                "target": "[2001:db8::7]:1161",
                "profile": "PotokUg405",
                "oids": [{ "name": "phase", "oid": spec.request().oids()[0].oid.to_string() }],
                "schedule": { "FixedDelay": { "delay": { "secs": 2, "nanos": 0 } } },
                "attempt": {
                    "timeout": { "secs": 0, "nanos": 750000000 },
                    "retries": 2,
                    "retry_delay": { "secs": 0, "nanos": 100000000 }
                }
            } }
        })
    );
    assert_no_credentials(&record);
    let restored: TaskDescription = serde_json::from_value(record.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), record);
    let mut legacy_record = record.clone();
    legacy_record["limit"] = json!(10);
    let restored: TaskDescription = serde_json::from_value(legacy_record).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), record);
}

#[test]
fn set_description_retains_binary_values_and_excludes_both_communities() {
    let description = TaskDescription::from(&set_task("write value"));
    let record = serde_json::to_value(&description).unwrap();
    assert_no_credentials(&record);
    assert_eq!(
        record["scenario"]["SnmpSet"]["sets"][0]["value"],
        json!({
            "OctetString": [0, 128, 255]
        })
    );
    let restored: TaskDescription = serde_json::from_value(record.clone()).unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), record);
    let ScenarioDescription::SnmpSet {
        target,
        profile,
        sets,
        schedule,
        attempt,
        ..
    } = restored.scenario
    else {
        panic!("expected SET description");
    };
    assert_eq!(target, "127.0.0.2:1162".parse().unwrap());
    assert_eq!(profile, Some(SnmpProfile::PotokUg405));
    assert_eq!(sets[0].oid.to_string(), "1.3.6.1.2.1.1.5.0");
    assert_eq!(schedule, PollSchedule::fixed_rate(Duration::from_millis(250)).unwrap());
    assert_eq!(attempt.retries(), 1);
}

#[tokio::test]
async fn queued_descriptions_keep_each_revision_after_consecutive_updates_and_removal() {
    let application = Application::new(AppConfig::default(), vec![]);
    let mut recorded = application.subscribe();
    let running = application.spawn();
    let handle = running.handle();
    let mut subscription = handle.subscribe_with_snapshot().await.unwrap();
    let configs = [get_task("initial"), set_task("updated"), get_task("final")];
    let expected: Vec<_> = configs
        .iter()
        .map(|config| serde_json::to_value(TaskDescription::from(config)).unwrap())
        .collect();

    let id = handle.add_task(configs[0].clone()).await.unwrap();
    let mut snapshots = vec![handle.get_snapshot().await.unwrap().tasks.remove(0)];
    for config in &configs[1..] {
        handle.update_task(id, config.clone()).await.unwrap();
        snapshots.push(handle.get_snapshot().await.unwrap().tasks.remove(0));
    }
    handle.remove_task(id).await.unwrap();
    assert!(handle.get_snapshot().await.unwrap().tasks.is_empty());

    let mut observed = Vec::new();
    for (index, expected) in expected.iter().enumerate() {
        let event = subscription.events.try_recv().unwrap();
        assert_eq!(event.metadata.task_id, id);
        assert_eq!(event.metadata.revision, SpecRevision::new(u64::try_from(index + 1).unwrap()));
        assert_eq!(
            event.metadata.seq,
            subscription.snapshot.next_seq + u64::try_from(index).unwrap()
        );
        let description = match &event.data {
            EventData::TaskRegistered(description) if index == 0 => description,
            EventData::TaskConfigurationChanged(description) if index > 0 => description,
            _ => panic!("unexpected configuration event"),
        };
        assert_eq!(serde_json::to_value(description).unwrap(), *expected);
        let snapshot = &snapshots[index];
        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.configuration.revision, event.metadata.revision);
        assert_eq!(serde_json::to_value(&snapshot.configuration.description).unwrap(), *expected);
        let configuration = serde_json::to_value(&snapshot.configuration).unwrap();
        assert_no_credentials(&configuration);
        let restored: TaskConfigurationSnapshot =
            serde_json::from_value(configuration.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), configuration);
        let json = serde_json::to_value(&event).unwrap();
        assert_no_credentials(&json);
        observed.push(json);
    }
    let removed = subscription.events.try_recv().unwrap();
    assert!(matches!(removed.data, EventData::TaskRemoved));
    assert_eq!(removed.metadata.revision, SpecRevision::new(3));
    observed.push(serde_json::to_value(removed).unwrap());

    running.shutdown().await.unwrap();
    let mut saved = Vec::new();
    while let Ok(event) = recorded.try_recv() {
        saved.push(serde_json::to_value(event).unwrap());
    }
    assert_eq!(saved, observed);
}

#[test]
fn task_snapshot_describes_custom_requests_and_owns_its_configuration() {
    let request = SnmpGetSpec::new(
        "[2001:db8::7]:1161".parse().unwrap(),
        GET_COMMUNITY.parse().unwrap(),
        None,
        vec![SnmpGetItem {
            name: Some("custom".to_string()),
            oid: SnmpOid::parse("1.3.6.1.4.1.999.123.0").unwrap(),
            parser: Some(|_| Ok(BusinessValue::Text("parsed".to_string()))),
        }],
    );
    let config = TaskConfig::try_new(
        "custom GET".to_string(),
        ScenarioSpec::snmp_get(
            request,
            PollSchedule::fixed_delay(Duration::from_secs(2)).unwrap(),
            AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap(),
        )
        .unwrap(),
        4,
    )
    .unwrap();
    let application = Application::new(
        AppConfig::default(),
        vec![InitialTask {
            config,
            limit: RunLimit::Unlimited,
        }],
    );
    let mut snapshot = application
        .subscribe_with_snapshot()
        .snapshot
        .tasks
        .remove(0);
    let record = serde_json::to_value(&snapshot.configuration).unwrap();
    assert_no_credentials(&record);
    assert!(!record.to_string().contains("parser"));
    assert!(!format!("{snapshot:?}").contains(GET_COMMUNITY));
    let ScenarioDescription::SnmpGet { target, oids, .. } =
        &mut snapshot.configuration.description.scenario
    else {
        panic!("expected custom GET description");
    };
    assert_eq!(*target, "[2001:db8::7]:1161".parse().unwrap());
    assert_eq!(oids[0].name.as_deref(), Some("custom"));
    assert_eq!(oids[0].oid.to_string(), "1.3.6.1.4.1.999.123.0");
    oids.clear();
    snapshot.configuration.description.name = "local edit".to_string();

    let fresh = application
        .subscribe_with_snapshot()
        .snapshot
        .tasks
        .remove(0);
    assert_eq!(serde_json::to_value(fresh.configuration).unwrap(), record);
}
