use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use derive_more::Display;
use itertools::Itertools;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio::{
    sync::{broadcast, oneshot},
    time::Duration,
};
use uuid::Uuid;

use crate::error::ApplicationError;
use crate::polling::worker::{PollWorker, WorkerCommand, WorkerId};
use crate::snmp::SnmpReadClientConfig;
use crate::snmp::adapters::SnmpReader;
use crate::snmp::parsers::site_id_ug405_potok;
use crate::snmp::registry::{UTC_REPLY_GN_OID, UTC_REPLY_SITE_ID_POTOK_OID};
use crate::{
    error::{CreateMonitorError, Error, ParseError, SnmpError},
    monitor::{
        TaskRepository,
        application::{
            TasksRepoCommand, TasksRepoManager,
            config::{AppConfig, Query, SnmpOidItem},
            tasksrepo_manager::TasksRepoResponse,
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
        registry::scn_required,
    },
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
}

impl Application {
    pub async fn new(config: AppConfig) -> Result<Self, Error> {
        let app_uid = ApplicationId::generate();

        let mut tasks_order = Vec::new();
        let mut tasks_binding: HashMap<TaskId, TaskWorker> = HashMap::new();

        let mut tasks_repo = TaskRepository::new_empty();

        let (worker_tx, worker_rx) = mpsc::channel(32);

        for (i, task) in config.tasks.iter().enumerate() {
            let poll_config = PollConfig {
                timeout: Duration::from_millis(task.poll_timings.timeout_ms),
                retries: task.poll_timings.retries,
                retry_delay: Duration::from_millis(task.poll_timings.retry_delay_ms),
            };

            let worker_id = WorkerId::new(i as u64);
            let (worker_cmd_tx, worker_cmd_rx) = mpsc::channel(32);

            //let poller_factory = PollerFactory::new(poll_config);
            let task_history = TaskHistory::new(task.deep_history);
            let name = task.name.clone();
            let mut subject = vec![format!("Poll interval: {}ms", task.interval_ms)];

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

                    let snmp_client_config = SnmpReadClientConfig {
                        target,
                        port,
                        community,
                        timeout: poll_config.timeout,
                        retries: poll_config.retries as u32,
                        retry_delay: poll_config.retry_delay,
                    };

                    let snmp_client = create_snmp_read_client(snmp_client_config).await?;

                    let sanitized_oids = sanitize_oids(&dto.oids, i, profile.as_ref())?;
                    let oids_to_request = format!(
                        "Snmp-get request. Oids to request({}):\n{}",
                        sanitized_oids.len(),
                        sanitized_oids
                            .iter()
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
                    subject.push(oids_to_request);

                    let adapter = SnmpReader::new(snmp_client, sanitized_oids, profile).await?;

                    let worker_interval = Duration::from_millis(task.interval_ms);
                    let worker = PollWorker::new(
                        worker_id,
                        adapter,
                        worker_interval,
                        poll_config,
                        worker_tx.clone(),
                        worker_cmd_rx,
                    );
                    let join_handle = tokio::spawn(worker.run());

                    //let worker = Worker::new(worker_id, poller, worker_interval);
                    //let join_handle = tokio::spawn(worker.run(worker_tx.clone(), worker_cmd_rx));
                    let worker_control = WorkerControl {
                        cmd_tx: worker_cmd_tx,
                        join_handle,
                    };

                    let meta = TaskMeta {
                        protocol: Protocol::Snmp,
                        type_query: TypeQuery::SnmpGet,
                        name,
                        subject: subject.into_iter().join("\n"),
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

        let (repo_cmd_tx, mut repo_cmd_rx) = mpsc::channel::<TasksRepoCommand>(32);

        let worker_to_uid: HashMap<WorkerId, TaskId> = tasks_binding
            .iter()
            .map(|(k, v)| (v.worker_id.clone(), k.clone()))
            .collect();

        let mut snapshot_manager =
            TasksRepoManager::new(tasks_repo, worker_to_uid, repo_cmd_rx, worker_rx);

        tokio::spawn(snapshot_manager.run());

        Ok(Self {
            app_uid,
            tasks_order,
            task_binding: tasks_binding,
            state: ApplicationState::Idle,
            tasksrepo_manager_tx: repo_cmd_tx,
            //repository_tx,
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

    pub async fn get_snapshot(&self) -> Result<TaskRepository, ApplicationError> {
        /*
                let (resp_tx, resp_rx) = oneshot::channel();

                self.tasksrepo_manager_tx
                    .send(TasksRepoCommand::GetSnapShot { response: resp_tx })
                    .await
                    .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()));
                resp_rx
                    .await
                    .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()))

        */

        let (resp_tx, resp_rx) = oneshot::channel();

        if let Err(_) = self
            .tasksrepo_manager_tx
            .send(TasksRepoCommand::GetSnapShot { response: resp_tx })
            .await
        {
            let reason = "Repository unawailable.".to_string();
            tracing::error!(target: "subscribe", "{}", &reason);
            return Err(ApplicationError::GetSnapshot { reason });
        }

        match resp_rx.await {
            Ok(subscribe) => {
                tracing::info!(target: "subscribe", "{}", "Snapshot was received successfully ".to_string());

                Ok(subscribe)
            }
            Err(_) => {
                let reason = "Timeout error from repository.".to_string();
                tracing::error!(target: "subscribe", "{}", &reason);
                Err(ApplicationError::GetSnapshot { reason })
            }
        }
    }

    pub fn id(&self) -> &ApplicationId {
        &self.app_uid
    }

    pub async fn subscribe(
        &self,
    ) -> Result<broadcast::Receiver<TasksRepoResponse>, ApplicationError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        if let Err(_) = self
            .tasksrepo_manager_tx
            .send(TasksRepoCommand::SubscribeForUpdate { response: resp_tx })
            .await
        {
            let reason = "Repository unawailable.".to_string();
            tracing::error!(target: "subscribe", "{}", &reason);
            return Err(ApplicationError::RepositorySubscribe { reason });
        }

        match resp_rx.await {
            Ok(subscribe) => {
                tracing::info!(target: "subscribe", "{}", "Subcrribe for task repository update succesfull".to_string());

                Ok(subscribe)
            }
            Err(_) => {
                let reason = "Timeout error from repository.".to_string();
                tracing::error!(target: "subscribe", "{}", &reason);
                Err(ApplicationError::RepositorySubscribe { reason })
            }
        }
    }
}

async fn create_snmp_read_client(
    config: SnmpReadClientConfig,
) -> Result<SnmpReadClient, CreateMonitorError> {
    SnmpReadClient::new(config)
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
            //let parser = resolve_oid_parser(&oid);
            Ok(SnmpGetQueryItem {
                name: item.name.clone(),
                oid,
                business_value_parser: None, // делегируем поиск парсера в 'реестре' адаптеру
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(sanitized_oids)
}

/*
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
*/

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
