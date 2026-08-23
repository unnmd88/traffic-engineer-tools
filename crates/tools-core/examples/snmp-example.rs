use anyhow::{Context, Result};
use std::any;
use std::net::{IpAddr, Ipv4Addr};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Duration;
use tools_core::monitor::application::app::Application;
use tools_core::monitor::application::config::{
    AppConfig, Query, QuerySnmpGet, TaskConfig, TaskPollTimings,
};

use tools_core::polling::PollConfig;
use tools_core::snmp::adapters::GenericCustomReader;
use tools_core::snmp::parsers::parse_oids;
use tools_core::snmp::primitives::{Community, Port, SnmpOid};
use tools_core::snmp::{SnmpGetQueryItem, SnmpReadClient, create_client, primitives};
use tools_core::worker::{PollerFactory, TaskEvent, TaskResult, Worker, WorkerId};
use tools_core::{Pollable, SnmpError};

#[tokio::main]
async fn main() -> Result<()> {
    let client = create_client(
        "127.0.0.1".parse::<IpAddr>().expect("Не Ip-адрес"),
        1161,
        Community::parse("public".to_string())?,
        Duration::from_millis(4000),
        0,
        Duration::from_millis(200),
    )
    .await?;

    let snmp_client = SnmpReadClient::new_with_client(client);

    println!("{}", "══".repeat(40));
    let oids = test_create_oids()?;
    println!("{}", "══".repeat(40));

    print_snmp_get(&snmp_client).await?;
    println!("{}", "══".repeat(40));

    print_snmp_get_many(&oids, &snmp_client).await?;
    println!("{}", "══".repeat(40));

    test_custom_snmp_adapter(&snmp_client).await?;
    println!("{}", "══".repeat(40));
    println!("{}", "══".repeat(40));
    println!("{}", "══".repeat(40));

    //test_worker(&snmp_client).await?;
    println!("{}", "\\\\\\\\".repeat(40));

    test_monitor_application().await;

    Ok(())
}

fn test_create_oids() -> Result<Vec<SnmpOid>> {
    let oids = parse_oids(&[
        "1.3.6.1.4.1.1618.3.7.2.11.2".to_string(),
        "1.3.6.1.4.1.1618.3.7.2.1.2".to_string(),
    ])
    .unwrap();
    println!("{:?}", oids);

    let stage = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.11.2").expect("Не Oid");
    let plan = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.1.2").expect("Не Oid");

    println!("{:?}", stage);
    println!("{:?}", plan);
    Ok(oids)
}

async fn print_snmp_get(snmp_client: &SnmpReadClient) -> Result<()> {
    let stage = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.11.2").expect("Не Oid");

    println!("Test SnmpClient `get`\n\n{:#?}", snmp_client.get(&stage).await?);
    Ok(())
}

async fn print_snmp_get_many(oids: &[SnmpOid], snmp_client: &SnmpReadClient) -> Result<()> {
    let res = snmp_client.get_many(&oids).await?;
    println!("Test SnmpClient `get_many`\n\n {:#?}", res);
    Ok(())
}

async fn test_custom_snmp_adapter(client: &SnmpReadClient) -> Result<()> {
    let stage = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.11.2").expect("Не Oid");
    let plan = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.1.2").expect("Не Oid");

    let items = vec![
        SnmpGetQueryItem {
            name: Some("Stage".to_string()),
            oid: stage,
        },
        SnmpGetQueryItem {
            name: None,
            oid: plan,
        },
    ];
    let custom_adapter = GenericCustomReader::new(client.clone(), items);

    let res = custom_adapter.poll().await?;
    println!("Test GenericCustomReader: \n{:#?}", res);

    Ok(())
}

async fn test_worker(snmp_client: &SnmpReadClient) -> Result<()> {
    let stage = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.11.2").expect("Не Oid");
    let plan = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.1.2").expect("Не Oid");

    let items = vec![
        SnmpGetQueryItem {
            name: Some("Stage".to_string()),
            oid: stage,
        },
        SnmpGetQueryItem {
            name: None,
            oid: plan,
        },
    ];
    let poll_config = PollConfig {
        timeout: Duration::from_millis(2000),
        retries: 4,
        retry_delay: Duration::from_millis(200),
    };
    let poller = PollerFactory::new(poll_config)
        .snmp_get_use_case(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            1161,
            Community::parse("public".to_string())?,
            items,
        )
        .await?;

    let worker = Worker::new(WorkerId(888), poller, Duration::from_millis(4000));

    let (tx, mut rx) = mpsc::channel::<TaskEvent>(100);

    let (tx_cmd, mut cmd_rx) = mpsc::channel(100);

    let worker2 = {
        let stage = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.11.2").expect("Не Oid");
        let plan = SnmpOid::parse("1.3.6.1.4.1.1618.3.7.2.1").expect("Не Oid");

        let items = vec![
            SnmpGetQueryItem {
                name: Some("Stage2".to_string()),
                oid: stage,
            },
            SnmpGetQueryItem {
                name: Some("Plan".to_string()),
                oid: plan,
            },
        ];
        let poll_config = PollConfig {
            timeout: Duration::from_millis(2000),
            retries: 3,
            retry_delay: Duration::from_millis(200),
        };
        let p = PollerFactory::new(poll_config)
            .snmp_get_use_case(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                1164,
                Community::parse("public".to_string())?,
                items,
            )
            .await?;
        let w = Worker::new(WorkerId(555), p, Duration::from_millis(5000));

        w
    };

    let (tx_cmd2, mut cmd_rx2) = mpsc::channel(100);
    tokio::spawn(worker2.run(tx.clone(), cmd_rx2));

    tokio::spawn(worker.run(tx.clone(), cmd_rx));

    while let Some(message) = rx.recv().await {
        println!("Получил WorkerMessage");
        println!("WorkerID: {:#?}", message.task_result);

        match message.task_result {
            TaskResult::SnmpGet(r) => println!("{r}"),
            _ => println!("NotImplemented"),
        }
    }
    Ok(())
}

async fn test_monitor_application() -> anyhow::Result<()> {
    let task_snmp_get_dto = QuerySnmpGet {
        host: "127.0.0.1".to_string(),
        port: 1161,
        community: "public".to_string(),
        oids: vec![
            SnmpOidItemDto {
                name: Some("Stage-Фаза".to_string()),
                oid: "1.3.6.1.4.1.1618.3.7.2.11.2".to_string(),
            },
            SnmpOidItemDto {
                name: Some("Plan-План".to_string()),
                oid: "1.3.6.1.4.1.1618.3.6.2.1.2".to_string(),
            },
        ],
    };

    let snmp_get_query = Query::SnmpGet(task_snmp_get_dto);
    let task = TaskConfig {
        name: "T-1".to_string(),
        poll_timings: TaskPollTimings {
            timeout_ms: 1000,
            retries: 2,
            retry_delay_ms: 200,
        },
        interval_ms: 4000,
        deep_history: 2,
        query: snmp_get_query,
    };
    println!("{:#?}", task);

    let group1 = TaskGroupConfig {
        name: "Group-Группа 1".to_string(),
        tasks: vec![task],
    };
    let config = AppConfig {
        tasks: vec![group1],
    };
    let mut app = Application::new(config).await?;
    let app_state = app.start().await;
    println!("Application State: {app_state}");

    println!("{}", app.id());
    println!("Is running: {}", app.is_running());
    println!("Current state: {}", app.current_state());
    println!("Mapping:\n{:#?}", app.tasks());

    return Ok(());

    loop {
        let snaphot = app.get_snapshot().await?;
        println!("Получил:\n{:#?}", snaphot);

        tokio::time::sleep(Duration::from_secs(4)).await;
    }

    tokio::signal::ctrl_c().await?;
    println!("Ctrl-C нажат");
    Ok(())
}
