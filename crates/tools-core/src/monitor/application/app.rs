use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use derive_more::Display;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio::{
    sync::{broadcast, oneshot},
    time::Duration,
};
use uuid::Uuid;

use crate::snmp::adapters::CustomReader;
use crate::{
    error::{CreateMonitorError, Error, ParseError, SnmpError},
    monitor::{
        TaskRepository,
        application::{
            TasksRepoCommand, TasksRepoManager,
            config::{AppConfig, Query, SnmpOidItem},
            tasksrepo_manager::TasksRepoEvent,
            worker_brige::WorkerBridge,
        },
        task::{Protocol, TaskHistory, TaskId, TaskMeta, TypeQuery},
    },
    polling::PollConfig,
    snmp::{
        SnmpGetQueryItem, SnmpReadClient,
        community::Community,
        oid::SnmpOid,
        parsers::{OidValueParserFn, parse_ug405_stage},
        profiles::SnmpProfile,
        registry::{scn_required, utcReplyGn},
    },
    worker::{PollerFactory, TaskEvent, Worker, WorkerCommand, WorkerId},
};

#[derive(Clone, Display)]
pub struct ApplicationId(Uuid);

impl ApplicationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Display, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationState {
    Idle,
    Runnig,
}

#[derive(Debug)]
struct WorkerControl {
    cmd_tx: mpsc::Sender<WorkerCommand>,
    join_handle: JoinHandle<()>,
}

#[derive(Debug)]
pub struct TaskWorker {
    worker_id: WorkerId,
    worker_control: WorkerControl,
}

pub struct Application {
    app_uid: ApplicationId,
    task_binding: HashMap<TaskId, TaskWorker>,
    tasks_order: Vec<TaskId>,
    tasksrepo_manager_tx: mpsc::Sender<TasksRepoCommand>,
    state: ApplicationState,
    subscriber_broadcast_tx: broadcast::Sender<TasksRepoEvent>,
}

impl Application {
    pub async fn new(config: AppConfig) -> Result<Self, Error> {
        let app_uid = ApplicationId::generate();

        let mut tasks_order = Vec::new();
        let mut tasks_binding: HashMap<TaskId, TaskWorker> = HashMap::new();

        let mut tasks_repo = TaskRepository::new_empty();

        let (worker_tx, worker_rx) = mpsc::channel::<TaskEvent>(32);

        for (i, task) in config.tasks.iter().enumerate() {
            let poll_config = PollConfig {
                timeout: Duration::from_millis(task.poll_timings.timeout_ms),
                retries: task.poll_timings.retries,
                retry_delay: Duration::from_millis(task.poll_timings.retry_delay_ms),
            };

            let worker_id = WorkerId::new(i as u64);
            let (worker_cmd_tx, worker_cmd_rx) = mpsc::channel(32);

            let poller_factory = PollerFactory::new(poll_config);
            let task_history = TaskHistory::new(task.deep_history);
            let name = task.name.clone();

            match &task.query {
                Query::SnmpGet(dto) => {
                    let target = parse_ip(&dto.host, i)?;
                    let port = dto.port;
                    let community = parse_snmp_community(&dto.community, i)?;
                    let profile = dto
                        .profile
                        .clone()
                        .map(|p| p.parse::<SnmpProfile>())
                        .transpose()
                        .map_err(|e| CreateMonitorError::InvalidSnmpProfile { message: e })?;

                    let snmp_client =
                        create_snmp_read_client(target, port, community.clone(), &poll_config)
                            .await?;

                    let sanitized_oids = sanitize_oids(&dto.oids, i, profile.as_ref())?;
                    let oids = resolve_oids(sanitized_oids, profile.as_ref(), &snmp_client).await?;

                    let subject = format!(
                        "Snmp-get request. Oids to request({}):\n{}",
                        oids.len(),
                        oids.iter()
                            .map(|item| {
                                format!(
                                    " - {}{}",
                                    item.oid,
                                    item.name
                                        .clone()
                                        .map_or_else(|| "".to_string(), |n| format!(" [{n}]"))
                                )
                            })
                            .collect::<Vec<String>>()
                            .join("\n")
                    );

                    let poller = poller_factory.snmp_get_use_case_with_client(snmp_client, oids);
                    let worker_interval = Duration::from_millis(task.interval_ms);
                    let worker = Worker::new(worker_id, poller, worker_interval);
                    let join_handle = tokio::spawn(worker.run(worker_tx.clone(), worker_cmd_rx));
                    let worker_control = WorkerControl {
                        cmd_tx: worker_cmd_tx,
                        join_handle,
                    };

                    let meta = TaskMeta {
                        protocol: Protocol::Snmp,
                        type_query: TypeQuery::SnmpGet,
                        name,
                        subject: subject,
                        target: format!("{target} port: {port}"),
                    };

                    let task_id = tasks_repo.add_task(meta, None, Some(task_history));
                    tasks_order.push(task_id.clone());
                    tasks_binding.insert(
                        task_id,
                        TaskWorker {
                            worker_id: worker_id.clone(),
                            worker_control,
                        },
                    );
                }
            }
        }

        // tx - для воркеров(они шлют в snapshot-менеджер уже)
        // rx - для самого snapshot-менеджера(он принимает от воркеров, обновляет snapshot и т.д.)
        let (snapshot_tx, snapshot_rx) = mpsc::channel(32);
        // Воркер-Бридж - это прокси, куда шлют результаты воркеры. Разбирает сообщение, маппит
        // id воркера на uid, оборачивает в enum TasksRepoCommand и отправляет дальше
        // в TasksRepoManager.
        //let mut worker_to_uid: HashMap<WorkerId, Uid> = HashMap::new();
        let worker_to_uid: HashMap<WorkerId, TaskId> = tasks_binding
            .iter()
            .map(|(k, v)| (v.worker_id.clone(), k.clone()))
            .collect();
        let workers_bridge = WorkerBridge::new(worker_to_uid);
        tokio::spawn(workers_bridge.run(snapshot_tx.clone(), worker_rx));
        let mut snapshot_manager = TasksRepoManager::new(tasks_repo);

        // Броадкаст для подписки пользователя на обновления репо задач.
        let (subscriber_broadcast_tx, _) = broadcast::channel(16);

        tokio::spawn(snapshot_manager.run(snapshot_rx, subscriber_broadcast_tx.clone()));

        Ok(Self {
            app_uid,
            tasks_order,
            task_binding: tasks_binding,
            state: ApplicationState::Idle,
            tasksrepo_manager_tx: snapshot_tx,
            subscriber_broadcast_tx,
        })
    }

    pub async fn start(&mut self) -> ApplicationState {
        if matches!(self.state, ApplicationState::Idle) {
            for tw in self.task_binding.values() {
                tw.worker_control.cmd_tx.send(WorkerCommand::Start).await;
            }
            self.state = ApplicationState::Runnig;
        }
        self.state
    }

    pub fn current_state(&self) -> ApplicationState {
        self.state
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, ApplicationState::Runnig)
    }

    pub fn tasks_order(&self) -> &[TaskId] {
        &self.tasks_order
    }

    pub async fn get_snapshot(&self) -> Result<TaskRepository, Error> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tasksrepo_manager_tx
            .send(TasksRepoCommand::GetSnapShot { response: resp_tx })
            .await
            .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()));
        resp_rx
            .await
            .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()))
    }

    pub fn id(&self) -> &ApplicationId {
        &self.app_uid
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TasksRepoEvent> {
        self.subscriber_broadcast_tx.subscribe()
    }
}

async fn create_snmp_read_client(
    target: IpAddr,
    port: u16,
    community: Community,
    poll_config: &PollConfig,
) -> Result<SnmpReadClient, CreateMonitorError> {
    SnmpReadClient::new(
        target,
        port,
        community,
        poll_config.timeout.clone(),
        (poll_config.retries.clone()) as u32,
        poll_config.retry_delay.clone(),
    )
    .await
    .map_err(|_| CreateMonitorError::SnmpClientCreate)
}

fn resolve_oid(
    raw: &str,
    profile: Option<&SnmpProfile>,
    task_idx: usize,
    pos: usize,
) -> Result<SnmpOid, CreateMonitorError> {
    let raw = raw.trim().to_lowercase();

    if let Ok(oid) = SnmpOid::parse(&raw) {
        return Ok(oid);
    }

    let profile = profile.ok_or(CreateMonitorError::SnmpProfileMustBeProvided {
        message: "SNMP profile is required for auto search oid by name".to_string(),
        task_idx,
    })?;

    let oid_str = profile
        .get_oid_by_alias(&raw)
        .ok_or(CreateMonitorError::UnknownAlias {
            task_idx,
            pos,
            alias: raw.clone(),
        })?;

    SnmpOid::parse(oid_str).map_err(|_| CreateMonitorError::InvalidSnmpOid {
        task_idx,
        pos,
        oid: oid_str.to_string(),
    })
}

fn resolve_oid_parser(oid: &SnmpOid) -> Option<OidValueParserFn> {
    let parser: OidValueParserFn = match oid.to_string().as_ref() {
        utcReplyGn => parse_ug405_stage,
        _ => return None,
    };

    Some(parser)
}

fn sanitize_oids(
    oids: &[SnmpOidItem],
    task_idx: usize,
    profile: Option<&SnmpProfile>,
) -> Result<Vec<SnmpGetQueryItem>, CreateMonitorError> {
    let sanitized_oids = oids
        .iter()
        .enumerate()
        .map(|(pos, item)| -> Result<SnmpGetQueryItem, CreateMonitorError> {
            let oid = resolve_oid(&item.oid, profile, task_idx, pos)?;
            let parser = resolve_oid_parser(&oid);
            Ok(SnmpGetQueryItem {
                name: item.name.clone(),
                oid,
                business_value_parser: parser,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(sanitized_oids)
}

async fn resolve_oids(
    oids: Vec<SnmpGetQueryItem>,
    profile: Option<&SnmpProfile>,
    client: &SnmpReadClient,
) -> Result<Vec<SnmpGetQueryItem>, CreateMonitorError> {
    let ascii_scn = if let Some(profile) = profile {
        profile
            .get_scn(&client)
            .await
            .map_err(|e| CreateMonitorError::ScnError {
                profile: (*profile).to_string(),
                message: e.to_string(),
            })?
    } else {
        None
    };

    if let Some(scn) = ascii_scn {
        let scn_as_str = scn.to_scn();
        let mut result = Vec::with_capacity(oids.len());

        for item in oids {
            let oid = if scn_required(&item.oid) {
                SnmpOid::parse(&format!("{}{}", item.oid, scn_as_str)).map_err(|e| {
                    tracing::error!(target: "resolve_oids", "Bug: {}", e);
                    CreateMonitorError::Other("Can`t resolve oids".to_string())
                })?
            } else {
                item.oid
            };
            result.push(SnmpGetQueryItem {
                name: item.name,
                oid,
                business_value_parser: item.business_value_parser,
            });
        }
        Ok(result)
    } else {
        Ok(oids)
    }
}

fn parse_ip(ip: &str, task_idx: usize) -> Result<IpAddr, CreateMonitorError> {
    ip.parse::<IpAddr>()
        .map_err(|r| CreateMonitorError::InvalidIpAddress {
            ip: ip.to_string(),
            task_idx,
        })
}

fn parse_snmp_community(community: &str, task_idx: usize) -> Result<Community, CreateMonitorError> {
    Community::parse(community.to_string()).map_err(|e| match e {
        ParseError::CantBeEmpty { name } => CreateMonitorError::SnmpCommunityIsEmpty { task_idx },
        ParseError::InvalidLength {
            message,
            min,
            max,
            provide: provided,
        } => CreateMonitorError::SnmpCommunityInvalidLength {
            task_idx: task_idx,
            min,
            max,
            provide: provided,
        },
        ParseError::Common { message } => CreateMonitorError::Other(message),
        _ => CreateMonitorError::Other("Can`t parse community string".to_string()),
    })
}

fn parse_oids(
    items: &[SnmpOidItem],
    task_idx: usize,
) -> Result<Vec<SnmpGetQueryItem>, CreateMonitorError> {
    let mut query_items = Vec::with_capacity(items.len());

    for (i, item) in items.iter().enumerate() {
        let parsed_oid = SnmpOid::parse(&item.oid).map_err(|e| {
            let oid_as_string = item.oid.to_string();
            match e {
                SnmpError::InvalidOid(o) => CreateMonitorError::InvalidSnmpOid {
                    task_idx: task_idx,
                    oid: item.oid.to_string(),
                    pos: i,
                },
                _ => CreateMonitorError::Other(format!(
                    "Parse oid({oid_as_string}) error: {e}. Task position: {task_idx}",
                )),
            }
        })?;
        query_items.push(SnmpGetQueryItem {
            oid: parsed_oid,
            name: item.name.clone(),
            business_value_parser: None,
        });
    }

    Ok(query_items)
}
