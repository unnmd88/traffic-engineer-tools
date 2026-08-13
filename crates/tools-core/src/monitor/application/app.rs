use core::task;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
};

use chrono::Local;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio::{
    sync::{broadcast, oneshot},
    time::Duration,
};
use uuid::Uuid;

use crate::{
    error::{CreateMonitorError, Error, ParseError, SnmpError},
    monitor::{
        Snapshot, Uid,
        application::{
            SnapshotCommand, SnapshotManager,
            config::{AppConfig, Query, SnmpOidItem},
            snapshot_manager::SnapshotEvent,
            task_mapping::{GroupMapping, Mapping, TaskMapping},
            worker_brige::WorkerBridge,
        },
        task::{Protocol, Task, TaskData, TaskHistory, TaskMeta, TypeQuery},
        taskgroup::{TaskGroup, TaskGroupId, TaskGroupName, TaskPosition},
    },
    polling::PollConfig,
    snmp::{
        SnmpQueryItem,
        primitives::{Community, SnmpOid},
    },
    worker::{Metrics, PollerFactory, TaskEvent, TaskResult, Worker, WorkerCommand, WorkerId},
};

struct IdGenerator {
    uid: u64,
    worker_id: u64,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            uid: 0,
            worker_id: 0,
        }
    }

    pub fn next_uid(&mut self) -> Uid {
        let id = self.worker_id;
        self.worker_id += 1;
        Uid(id)
    }

    pub fn next_worker_id(&mut self) -> WorkerId {
        let id = self.worker_id;
        self.worker_id += 1;
        WorkerId(id)
    }
}

struct WorkerTaskMapping {
    group_id: TaskGroupId,
    position: TaskPosition,
}

struct WorkerControl {
    cmd_tx: mpsc::Sender<WorkerCommand>,
    join_handle: JoinHandle<()>,
}

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

pub struct Application {
    app_uid: ApplicationId,
    mapping: Mapping,
    //snapshot: Snapshot,
    //uid_to_task: HashMap<Uid, (TaskGroupId, TaskPosition)>,
    uid_to_worker_control: HashMap<Uid, WorkerControl>,
    snapgot_manager_tx: mpsc::Sender<SnapshotCommand>,
    state: ApplicationState,
    subscriber_broadcast_tx: broadcast::Sender<SnapshotEvent>,
    //worker_mapping: HashMap<WorkerId, WorkerTaskMapping>,
    //worker_to_uid: HashMap<WorkerId, Uid>,
    //tx: mpsc::Sender<TaskEvent>,
    //rx: mpsc::Receiver<TaskEvent>,
}

impl Application {
    pub async fn new(config: AppConfig) -> Result<Self, Error> {
        let app_uid = ApplicationId::generate();
        let mut mapping_groups = Vec::new();

        let mut snapshot = Snapshot::new_empty();
        let mut id_generator = IdGenerator::new();
        let mut uid_to_task: HashMap<Uid, (TaskGroupId, TaskPosition)> = HashMap::new();
        let mut uid_to_worker_control: HashMap<Uid, WorkerControl> = HashMap::new();
        let mut worker_to_uid: HashMap<WorkerId, Uid> = HashMap::new();

        let (worker_tx, worker_rx) = mpsc::channel::<TaskEvent>(32);

        for (task_group_idx, group) in config.groups.iter().enumerate() {
            let task_group_name = TaskGroupName::parse(&group.name).map_err(|e| match e {
                ParseError::CantBeEmpty { name } => {
                    CreateMonitorError::GroupNameCantBeEmpty { id: task_group_idx }
                }
                ParseError::InvalidLength {
                    message,
                    min,
                    max,
                    provide,
                } => {
                    let group_name = group.name.to_string();
                    CreateMonitorError::GroupNameInvalidLength {
                        group_id: task_group_idx,
                        group_name,
                        min,
                        max,
                        provide,
                    }
                }
            })?;
            let task_group_id = snapshot.add_group(TaskGroup::new_empty(task_group_name.clone()));
            let mut task_mapping = Vec::new();

            for (task_idx, task) in group.tasks.iter().enumerate() {
                let poll_config = PollConfig {
                    timeout: Duration::from_millis(task.poll_timings.timeout_ms),
                    retries: task.poll_timings.retries,
                    retry_delay: Duration::from_millis(task.poll_timings.retry_delay_ms),
                };

                let poller_factory = PollerFactory::new(poll_config);
                let deep_history = task.deep_history;
                let name = task.name.clone();

                let worker_id = id_generator.next_worker_id();
                let task_uid = id_generator.next_uid();

                task_mapping.push(TaskMapping {
                    uid: task_uid.clone(),
                    name: name.clone(),
                });

                let (worker_cmd_tx, worker_cmd_rx) = mpsc::channel(32);

                match &task.query {
                    Query::SnmpGet(dto) => {
                        let target =
                            parse_ip(&dto.host, &task_group_name, task_group_idx, task_idx)?;
                        let port = dto.port;
                        let community = parse_snmp_community(
                            &dto.community,
                            &task_group_name,
                            task_group_idx,
                            task_idx,
                        )?;
                        let oids =
                            parse_oids(&dto.oids, &task_group_name, task_group_idx, task_idx)?;
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
                        let meta = TaskMeta {
                            protocol: Protocol::Snmp,
                            type_query: TypeQuery::SnmpGet,
                            name,
                            subject: subject,
                            target: format!("{target} port: {port}"),
                        };
                        let history = TaskHistory::new(deep_history);
                        let task_state = Task {
                            meta: meta,
                            data: TaskData {
                                result: TaskResult::Idle,
                                metrics: Metrics::default(),
                                last_update: Local::now(),
                            },
                            history,
                        };

                        let task_position = snapshot
                            .get_mut_taskgroup(&task_group_id)
                            .ok_or(CreateMonitorError::Other(format!(
                                "Snapshot is corrupted: taskgroup id out of range."
                            )))?
                            .add_taskstate(task_state);

                        let poller = poller_factory
                            .snmp_get_use_case(target, port, community, oids)
                            .await?;
                        let worker =
                            Worker::new(worker_id, poller, Duration::from_millis(task.interval_ms));
                        let join_handle =
                            tokio::spawn(worker.run(worker_tx.clone(), worker_cmd_rx));
                        let worker_control = WorkerControl {
                            cmd_tx: worker_cmd_tx,
                            join_handle,
                        };

                        uid_to_worker_control.insert(task_uid, worker_control);
                        uid_to_task.insert(task_uid, (task_group_id, task_position));
                        worker_to_uid.insert(worker_id, task_uid);
                    }
                }
            }

            mapping_groups.push(GroupMapping {
                tasks: task_mapping,
                name: group.name.clone(),
            });
        }

        let mapping = Mapping {
            groups: mapping_groups,
        };

        // tx - для воркеров(они шлют в snapshot-менеджер уже)
        // rx - для самого snapshot-менеджера(он принимает от воркеров, обновляет snapshot и т.д.)
        let (snapshot_tx, snapshot_rx) = mpsc::channel(32);
        // Воркер-Бридж - это прокси, куда шлют результаты воркеры. Разбирает сообщение, маппит
        // id воркера на uid, оборачивает в enum SnapshotCommand и отправляет дальше
        // в SnapshotManager.
        let workers_bridge = WorkerBridge::new(worker_to_uid);
        tokio::spawn(workers_bridge.run(snapshot_tx.clone(), worker_rx));
        let mut snapshot_manager = SnapshotManager::new(snapshot, uid_to_task);

        // Броадкаст для подписки пользователя на обновления снепшота.
        let (subscriber_broadcast_tx, _) = broadcast::channel(16);

        tokio::spawn(snapshot_manager.run(snapshot_rx, subscriber_broadcast_tx.clone()));

        Ok(Self {
            app_uid,
            //uid_to_task,
            mapping,
            state: ApplicationState::Idle,
            uid_to_worker_control,
            snapgot_manager_tx: snapshot_tx,
            subscriber_broadcast_tx,
        })
    }

    pub async fn start(&mut self) -> ApplicationState {
        if matches!(self.state, ApplicationState::Idle) {
            for w in self.uid_to_worker_control.values() {
                w.cmd_tx.send(WorkerCommand::Start).await;
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

    pub fn tasks_mapping(&self) -> &Mapping {
        &self.mapping
    }

    pub async fn get_snapshot(&self) -> Result<Snapshot, Error> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.snapgot_manager_tx
            .send(SnapshotCommand::GetSnapShot { response: resp_tx })
            .await
            .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()));
        resp_rx
            .await
            .map_err(|_| Error::NoResponse("Can`t get Snapshot".to_string()))
    }

    pub fn id(&self) -> &ApplicationId {
        &self.app_uid
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SnapshotEvent> {
        self.subscriber_broadcast_tx.subscribe()
    }
}

struct TaskParseData<'a> {
    group_name: &'a TaskGroupName,
    group_id: usize,
    task_id: usize,
}

fn parse_ip(
    ip: &str,
    group_name: &TaskGroupName,
    group_idx: usize,
    task_idx: usize,
) -> Result<IpAddr, CreateMonitorError> {
    ip.parse::<IpAddr>()
        .map_err(|r| CreateMonitorError::InvalidIpAddress {
            ip: ip.to_string(),
            group_id: group_idx,
            group_name: group_name.to_string(),
            task_id: task_idx,
        })
}

fn parse_snmp_community(
    community: &str,
    group_name: &TaskGroupName,
    group_idx: usize,
    task_idx: usize,
) -> Result<Community, CreateMonitorError> {
    Community::parse(community.to_string()).map_err(|e| {
        let group_name = group_name.to_string();
        match e {
            ParseError::CantBeEmpty { name } => CreateMonitorError::SnmpCommunityIsEmpty {
                task_idx,
                group_idx,
                group_name,
            },
            ParseError::InvalidLength {
                message,
                min,
                max,
                provide: provided,
            } => CreateMonitorError::SnmpCommunityInvalidLength {
                group_id: group_idx,
                group_name,
                task_id: task_idx,
                min,
                max,
                provide: provided,
            },
        }
    })
}

fn parse_oids(
    items: &[SnmpOidItem],
    group_name: &TaskGroupName,
    group_idx: usize,
    task_idx: usize,
) -> Result<Vec<SnmpQueryItem>, CreateMonitorError> {
    let mut query_items = Vec::with_capacity(items.len());

    for (i, item) in items.iter().enumerate() {
        let parsed_oid = SnmpOid::parse(&item.oid).map_err(|e| {
            let group_name = group_name.to_string();
            let oid_as_string =  item.oid.to_string();
            match e {
                SnmpError::InvalidOid(o) => {
                    CreateMonitorError::InvalidSnmpOid { group_id: group_idx, group_name, task_id: task_idx, oid: item.oid.to_string() , pos: i }

                }
                _ => CreateMonitorError::Other(format!("Parse oid({oid_as_string}) error: {e}. GroupName: {group_name} group id: {group_idx} task id: {task_idx}", ))
            }
        })?;
        query_items.push(SnmpQueryItem {
            oid: parsed_oid,
            name: item.name.clone(),
        });
    }

    Ok(query_items)
}
