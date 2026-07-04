use anyhow::Result;
use net_monitor::cli::{Cli, Command, OutputFormat};
use net_monitor::config::{
    Config, IndependentStrategyConfig, Provider, SynchronizedStrategyConfig,
};
use net_monitor::icmp::IcmpProvider;
use net_monitor::log_parser::LogParser;
use net_monitor::models::{
    AboutApp, Event, IndependentConfigDetails, Strategy, TracerouteEngine,
};
use net_monitor::poller::{IndependentPoller, PollConfig, SynchronizedPoller};
use net_monitor::sender::{EventSender, JsonSender};
use net_monitor::snmp::SnmpProvider;
use net_monitor::traceroute::TrippyTracertProvider;
use net_monitor::traits::{Pollable, TracerouteProvider};
use net_monitor::utils::get_session_id;
use net_monitor::version;
use net_monitor::{event_loop, logging};

use clap::Parser;
use std::time::Duration;
use std::{fs::File, net::IpAddr};
use tokio::sync::mpsc::{self};
use tracing::{error, info};

type Senders = Vec<Box<dyn EventSender + Send>>;

const CONFIG_ERROR_MSG: &str = "Ошибка конфигурации.\n";
// const CONFIG_NAME: &str = "config.toml";
const INIT_SUCCESSFULLY: &str = "init successfully.";
const INIT_FAILED: &str = "init failed.";
const ICMP_PROVIDER: &str = "IcmpProvider";
const SNMP_PROVIDER: &str = "SnmpProvider";
const ERROR_READING_CONFIG: &str = "Error reading configuration file";
const ERROR_INIT_ICMP: &str = "❌ Ошибка инициализации ICMP";
const ERROR_INIT_SNMP: &str = "❌ Ошибка инициализации SNMP";

/*
fn load_config(path: &str) -> Result<Config, String> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        error!("{ERROR_READING_CONFIG} '{path}': {e}");
        format!("❌ Не удалось прочитать '{path}': {}", e)
    })?;

    let config = toml::from_str(&contents).map_err(|e| {
        error!("{ERROR_READING_CONFIG} '{path}': {e}");
        format!("❌ Ошибка в {CONFIG_NAME}: {}", user_friendly_error(&e))
    })?;
    info!("Config {path} loaded successfully.");

    Ok(config)
}

fn user_friendly_error(e: &toml::de::Error) -> String {
    let msg = e.message();

    if msg.contains("missing field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!("отсутствует поле '{}'", field);
        }
        return "отсутствует обязательное поле".to_string();
    }

    if msg.contains("unknown field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!(
                "неизвестное поле '{}' — проверьте название",
                field
            );
        }
    }

    if msg.contains("invalid type") {
        return "неверный тип значения (например, строка вместо числа)"
            .to_string();
    }

    msg.to_string()
}


fn init_tracert(config: &Config) -> Result<Option<TracertProvider>, String> {
    let tracert = if config.ping.fallback_tracert {
        match TracertProvider::new(
            config.network.target,
            config.tracert.max_hops,
            config.tracert.queries_per_hop,
        ) {
            Ok(t) => Some(t),
            Err(e) => {
                error!("Tracert {}: {}", INIT_FAILED, e);
                return Err(e);
            }
        }
    } else {
        None
    };
    Ok(tracert)
}
*/

async fn init_icmp_provider(
    username: String,
    target: IpAddr,
    timeout_ms: u64,
) -> Result<IcmpProvider, String> {
    //let tracert = init_tracert(&config)?;

    let icmp_provider = match IcmpProvider::new(username, target, timeout_ms) {
        Ok(provider) => provider,
        Err(e) => {
            let err = format!("{ICMP_PROVIDER} {INIT_FAILED}: {e}");
            error!("{}", err);
            return Err(err);
        }
    };

    info!("{ICMP_PROVIDER} {INIT_SUCCESSFULLY}");
    Ok(icmp_provider)
}

async fn init_tracert_provider(
    engine: TracerouteEngine,
    username: String,
    target: IpAddr,
    timeout_ms: u64,
    max_hops: u8,
    queries_per_hop: u8,
) -> Result<Box<dyn Pollable>, String> {
    let p = match engine {
        TracerouteEngine::Trippy => {
            let provider = TrippyTracertProvider::new(
                username,
                target,
                max_hops,
                queries_per_hop,
            )
            .await;
            Box::new(provider)
        }
        TracerouteEngine::System => {
            eprintln!("TracerouteEngine::System not implemented");
            error!("TracerouteEngine::System not implemented");
            std::process::exit(1)
        }
    };
    Ok(p)
}

async fn init_snmp_provider(
    username: String,
    target: IpAddr,
    timeout_ms: u64,
    port: u16,
    community: String,
    oids: Vec<String>,
) -> Result<SnmpProvider, String> {
    let snmp_provider = match SnmpProvider::new(
        username, target, port, community, oids, timeout_ms,
    )
    .await
    {
        Ok(provider) => provider,
        Err(e) => {
            let err = format!("{SNMP_PROVIDER} {INIT_FAILED}: {e}");
            error!("{}", err);
            return Err(err);
        }
    };
    info!("{SNMP_PROVIDER} {INIT_SUCCESSFULLY}.");
    Ok(snmp_provider)
}

fn create_independent_poller(
    provider: Box<dyn Pollable>,
    interval_ms: u64,
    poll_config: PollConfig,
    fallback: Option<Box<dyn Pollable>>,
    tx: mpsc::Sender<Event>,
) -> IndependentPoller {
    let provider_whoami = provider.whoami();
    let poller = IndependentPoller::new(
        provider,
        interval_ms,
        poll_config,
        fallback,
        tx,
    );
    info!(
        "IndependentPoller created successfully. Provider: {provider_whoami}",
    );
    poller
}
async fn try_create_fallback(
    fb_provider: Option<Provider>,
) -> Result<Option<Box<dyn Pollable>>, String> {
    let fb: Option<Box<dyn Pollable>> = match fb_provider {
        Some(fallback) => match fallback {
            Provider::Ping(cfg) => {
                let provider =
                    init_icmp_provider(cfg.name, cfg.target, cfg.timeout_ms)
                        .await?;
                Some(Box::new(provider))
            }
            Provider::Snmp(cfg) => {
                let provider = init_snmp_provider(
                    cfg.name,
                    cfg.target,
                    cfg.timeout_ms,
                    cfg.port,
                    cfg.community.clone(),
                    cfg.oids.clone(),
                )
                .await?;
                Some(Box::new(provider))
            }
            Provider::Tracert(cfg) => {
                let provider = init_tracert_provider(
                    cfg.engine,
                    cfg.name,
                    cfg.target,
                    cfg.timeout_ms,
                    cfg.max_hops,
                    cfg.queries_per_hop,
                )
                .await?;
                Some(provider)
            }
        },
        None => None,
    };
    Ok(fb)
}

async fn setup_independent_strategy(
    is_enabled: bool,
    independent_config: IndependentStrategyConfig,
    //pollers: &mut Vec<IndependentPoller>,
    tx: &mpsc::Sender<Event>,
) -> Result<Vec<IndependentPoller>, String> {
    let mut pollers: Vec<IndependentPoller> = Vec::new();

    if is_enabled {
        println!("Настройка стратегии: {}", Strategy::Independent);
        info!("Setup {:?} strategy.", Strategy::Independent);
        for ping_instance in independent_config.ping {
            if !ping_instance.enabled {
                continue;
            }

            let provider = init_icmp_provider(
                ping_instance.config.name,
                ping_instance.config.target,
                ping_instance.config.timeout_ms,
            )
            .await?;

            let poll_config = PollConfig {
                retries: ping_instance.retries,
                retries_delay_ms: ping_instance.retries_delay_ms,
            };

            let fb: Option<Box<dyn Pollable>> =
                try_create_fallback(ping_instance.fallback).await?;

            let poller = create_independent_poller(
                Box::new(provider),
                ping_instance.interval_ms,
                poll_config,
                fb,
                tx.clone(),
            );
            pollers.push(poller);
        }

        for snmp_instance in independent_config.snmp {
            let provider = init_snmp_provider(
                snmp_instance.config.name,
                snmp_instance.config.target,
                snmp_instance.config.timeout_ms,
                snmp_instance.config.port,
                snmp_instance.config.community.clone(),
                snmp_instance.config.oids.clone(),
            )
            .await?;
            let timings = PollConfig {
                retries: snmp_instance.retries,
                retries_delay_ms: snmp_instance.retries_delay_ms,
            };

            let fb: Option<Box<dyn Pollable>> =
                try_create_fallback(snmp_instance.fallback).await?;

            let poller = create_independent_poller(
                Box::new(provider),
                snmp_instance.interval_ms,
                timings,
                fb,
                tx.clone(),
            );
            pollers.push(poller);
        }

        let config_details = IndependentConfigDetails::new(
            pollers
                .iter()
                .map(|poller| poller.dump())
                .collect(),
        )
        .as_json()
        .map_err(|e| {
            error!("Failed to serialize independent config: {}", e);
            e.to_string()
        })?;

        let config_event = Event::Config {
            strategy: Strategy::Independent,
            details: config_details,
        };
        tx.send(config_event)
            .await
            .map_err(|e| e.to_string())?;
    }

    Ok(pollers)
}

async fn setup_synchronized_strategy(
    is_enabled: bool,
    synchronized_config: SynchronizedStrategyConfig,
    tx: &mpsc::Sender<Event>,
) -> Result<Option<SynchronizedPoller>, String> {
    let mut synchronized_poller: Option<SynchronizedPoller> = None;

    if is_enabled {
        println!("Настройка стратегии: {}", Strategy::Synchronized);
        println!(">> is_enabled: {is_enabled}");
        info!("Setup {:?} strategy.", Strategy::Synchronized);

        let mut providers: Vec<(
            Box<dyn Pollable>,
            PollConfig,
            Option<Box<dyn Pollable>>,
        )> = Vec::new();

        for ping_instance in synchronized_config.ping {
            let provider = init_icmp_provider(
                ping_instance.config.name,
                ping_instance.config.target,
                ping_instance.config.timeout_ms,
            )
            .await?;
            let poll_config = PollConfig {
                retries: ping_instance.retries,
                retries_delay_ms: ping_instance.retries_delay_ms,
            };
            let provider_whoami = provider.whoami();
            let fb: Option<Box<dyn Pollable>> =
                try_create_fallback(ping_instance.fallback).await?;
            providers.push((Box::new(provider), poll_config, fb));
            info!(
                "Added {} provider for {} strategy.",
                provider_whoami,
                Strategy::Synchronized
            );
        }

        for snmp_instance in synchronized_config.snmp {
            let provider = init_snmp_provider(
                snmp_instance.config.name,
                snmp_instance.config.target,
                snmp_instance.config.timeout_ms,
                snmp_instance.config.port,
                snmp_instance.config.community.clone(),
                snmp_instance.config.oids.clone(),
            )
            .await?;
            let poll_config = PollConfig {
                retries: snmp_instance.retries,
                retries_delay_ms: snmp_instance.retries_delay_ms,
            };
            let fb: Option<Box<dyn Pollable>> =
                try_create_fallback(snmp_instance.fallback).await?;
            providers.push((Box::new(provider), poll_config, fb));
        }

        let poller = SynchronizedPoller::new(
            providers,
            synchronized_config.interval_ms,
            tx.clone(),
        );

        info!(
            "{} strtegy poller created successfully.",
            Strategy::Synchronized
        );

        let config_details = poller.dump().as_json().map_err(|e| {
            error!("Failed to serialize synchronized config: {}", e);
            e.to_string()
        })?;

        let config_event = Event::Config {
            strategy: Strategy::Synchronized,
            details: config_details,
        };

        tx.send(config_event)
            .await
            .map_err(|e| e.to_string())?;

        synchronized_poller = Some(poller);
    }
    Ok(synchronized_poller)
}

fn spawn_independent_poll(
    poller: IndependentPoller,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(poller.run())
}

fn spawn_synchronized_poll(
    poller: SynchronizedPoller,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(poller.run())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Start { config } => {
            let _guard = logging::init_tracing();
            info!("{} Setup new monitor... {}", "#".repeat(40), "#".repeat(40));
            println!("Настраиваю монитор опроса...");
            let session_id = get_session_id();
            info!("Session id={session_id}.");

            tokio::time::sleep(Duration::from_millis(800)).await;

            let config = match Config::from_file(&config) {
                Ok(c) => c,
                Err(e) => {
                    error!("Setup monitor stopped: {}", e);
                    eprintln!("{}", e);
                    std::process::exit(1);
                }
            };

            if let Err(user_err_message) = app(config, session_id).await {
                error!("Setup monitor stopped.");
                eprintln!("{}", user_err_message);
                std::process::exit(1);
            }
        }

        Command::GenerateConfig {
            output,
            force,
            show,
        } => {
            if let Err(e) = Config::generate_default(&output, force, show) {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        Command::ProcessLog { log, format } => {
            let source = match File::open(&log) {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("Не удалось открыть файл: {}", e);
                    std::process::exit(1);
                }
            };
            println!("Файл лога прочитан успешно.");

            let parser = LogParser::new(source);

            match format {
                OutputFormat::Console => match parser.to_console() {
                    Ok(stats) => {
                        println!("Лог обработан.\n{}", stats.view_as_string())
                    }
                    Err(e) => {
                        error!("parse to console failed: {}", &e);
                        eprintln!("Ошибка: {}", &e)
                    }
                },
                OutputFormat::Csv => match parser.to_csv() {
                    Ok(details) => println!(
                        "Лог обработан.\n{}\nСозданный csv файл: `{}`",
                        details.stats.view_as_string(),
                        details.path,
                    ),
                    Err(e) => {
                        error!("parse to csv failed: {}", &e);
                        eprintln!("Ошибка: {}", &e)
                    }
                },
            }
        }
    }
}

async fn app(config: Config, session_id: String) -> Result<(), String> {
    let json_sender =
        match JsonSender::new(&config.log, session_id.clone()).await {
            Ok(sender) => {
                info!("JsonSender create successfully.");
                sender
            }
            Err(e) => {
                error!("Failed to create JsonSender: {e}");
                return Err(CONFIG_ERROR_MSG.to_string());
            }
        };

    let senders: Senders = vec![Box::new(json_sender)];

    // Канал, которые принимает структуры PollEvent и отправляет их различным senders.
    // tx -> передатчик структур PollEvent в канал, rx -> приёмник структур PollEvent.
    let (tx, rx) = mpsc::channel::<Event>(256);
    tokio::spawn(event_loop::handle_events(rx, senders));
    tokio::time::sleep(Duration::from_millis(20)).await;

    let app = Event::StartApplication {
        details: AboutApp {
            name: version::NAME.to_string(),
            version: version::VERSION.to_string(),
            description: version::DESCRIPTION.to_string(),
        },
    };
    info!("Application ver={}", version::VERSION);
    let _ = tx
        .send(app)
        .await
        .map_err(|e| e.to_string());

    // let mut independent_pollers: Vec<IndependentPoller> = Vec::new();
    // let mut synchronized_poller: Option<SynchronizedPoller> = None;
    let mut spawned_tasks = Vec::new();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let independent_pollers = setup_independent_strategy(
        config.independent_enabled,
        config.independent,
        &tx,
    )
    .await?;

    for p in independent_pollers {
        spawned_tasks.push(spawn_independent_poll(p));
    }

    if let Some(p) = setup_synchronized_strategy(
        config.synchronized_enabled,
        config.synchronized,
        &tx,
    )
    .await?
    {
        spawned_tasks.push(spawn_synchronized_poll(p));
    }

    if spawned_tasks.is_empty() {
        return Err("Нет задач для опроса. Монитор не запущен.".to_string());
    }
    let cnt_tasks = spawned_tasks.len();
    info!("Number of polling tasks: {cnt_tasks}");
    println!(
        "Файл для записи лога: {}\nКоличество опросов: {}",
        &config.log, cnt_tasks,
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("Monitor started.");
    println!("Монитор запущен.");
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("Нажмите Ctrl-C для остановки монитора.");
    tokio::time::sleep(Duration::from_millis(100)).await;

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down (Ctrl+C received)...");
            Ok(())
        }
        Err(e) => {
            error!("Failed to listen for Ctrl+C: {}", e);
            Err(format!("Ошибка системы: {}", e))
        }
    }
}
