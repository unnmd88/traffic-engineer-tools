use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

use crate::{
    monitor::{
        adapter::{Adapter, AdapterOutput, Query},
        task::TaskId,
    },
    polling::{PollConfig, Response, poll},
};

/// Эпоха воркера: монотонный номер, уникальный для каждого спавна задачи.
/// Newtype — чтобы не перепутать с `TaskId`/`SpecRevision`/счётчиком backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(u64);

impl Generation {
    pub fn first() -> Self {
        Self(0)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Команда оркестратора воркеру: только управление темпом, не состояние задачи.
#[derive(Debug)]
pub enum WorkerCmd {
    Pause,
    Resume,
}

/// Отчёт воркера оркестратору. `generation` позволяет оркестратору отбросить
/// отчёт от уже неактуального воркера (пересоздан или убит, а отчёт в очереди).
#[derive(Debug)]
pub enum WorkerReport {
    Built {
        task_id: TaskId,
        generation: Generation,
    },
    BuildFailed {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    BuildTimeout {
        task_id: TaskId,
        generation: Generation,
    },
    BuildPanicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    Polled {
        task_id: TaskId,
        generation: Generation,
        response: Response<AdapterOutput>,
    },
    PollFailed {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    PollPanicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    WorkerPanicked {
        task_id: TaskId,
        generation: Generation,
        reason: String,
    },
    Paused {
        task_id: TaskId,
        generation: Generation,
    },
    Resumed {
        task_id: TaskId,
        generation: Generation,
    },
}

impl WorkerReport {
    /// Ключ для фильтрации устаревших отчётов.
    pub fn key(&self) -> (TaskId, Generation) {
        match self {
            Self::Built {
                task_id,
                generation,
            }
            | Self::BuildFailed {
                task_id,
                generation,
                ..
            }
            | Self::BuildTimeout {
                task_id,
                generation,
            }
            | Self::BuildPanicked {
                task_id,
                generation,
                ..
            }
            | Self::Polled {
                task_id,
                generation,
                ..
            }
            | Self::PollFailed {
                task_id,
                generation,
                ..
            }
            | Self::PollPanicked {
                task_id,
                generation,
                ..
            }
            | Self::WorkerPanicked {
                task_id,
                generation,
                ..
            }
            | Self::Paused {
                task_id,
                generation,
            }
            | Self::Resumed {
                task_id,
                generation,
            } => (*task_id, *generation),
        }
    }
}

pub struct Worker {
    task_id: TaskId,
    generation: Generation,
    query: Query,
    poll: PollConfig,
    build_timeout: Duration,
    report_tx: mpsc::Sender<WorkerReport>,
    cmd_rx: mpsc::Receiver<WorkerCmd>,
}

impl Worker {
    pub fn new(
        task_id: TaskId,
        generation: Generation,
        query: Query,
        poll: PollConfig,
        build_timeout: Duration,
        report_tx: mpsc::Sender<WorkerReport>,
        cmd_rx: mpsc::Receiver<WorkerCmd>,
    ) -> Self {
        Self {
            task_id,
            generation,
            query,
            poll,
            build_timeout,
            report_tx,
            cmd_rx,
        }
    }

    pub async fn run(self) {
        let task_id = self.task_id;
        let generation = self.generation;
        let report_tx = self.report_tx.clone();
        if let Err(panic) = AssertUnwindSafe(self.run_inner()).catch_unwind().await {
            let _ = report_tx
                .send(WorkerReport::WorkerPanicked {
                    task_id,
                    generation,
                    reason: panic_message(panic),
                })
                .await;
        }
    }

    async fn run_inner(mut self) {
        let attempt = self.poll.attempt();

        let built = AssertUnwindSafe(tokio::time::timeout(
            self.build_timeout,
            Adapter::build(self.query, attempt),
        ))
        .catch_unwind()
        .await;

        let built = match built {
            Ok(res) => res,
            Err(panic) => {
                let _ = self
                    .report_tx
                    .send(WorkerReport::BuildPanicked {
                        task_id: self.task_id,
                        generation: self.generation,
                        reason: panic_message(panic),
                    })
                    .await;
                return;
            }
        };

        let adapter = match built {
            Ok(Ok(adapter)) => adapter,
            Ok(Err(err)) => {
                let _ = self
                    .report_tx
                    .send(WorkerReport::BuildFailed {
                        task_id: self.task_id,
                        generation: self.generation,
                        reason: err.to_string(),
                    })
                    .await;
                return;
            }
            Err(_elapsed) => {
                let _ = self
                    .report_tx
                    .send(WorkerReport::BuildTimeout {
                        task_id: self.task_id,
                        generation: self.generation,
                    })
                    .await;
                return;
            }
        };

        // 2) готов опрашивать
        let _ = self
            .report_tx
            .send(WorkerReport::Built {
                task_id: self.task_id,
                generation: self.generation,
            })
            .await;

        let interval = self.poll.interval();
        let mut paused = false;

        loop {
            if paused {
                match self.cmd_rx.recv().await {
                    Some(WorkerCmd::Resume) => {
                        paused = false;
                        let _ = self
                            .report_tx
                            .send(WorkerReport::Resumed {
                                task_id: self.task_id,
                                generation: self.generation,
                            })
                            .await;
                    }
                    Some(WorkerCmd::Pause) => {}
                    None => return,
                }
                continue;
            }

            tokio::select! {
                _ = sleep(interval) => {
                    let polled = AssertUnwindSafe(poll(&attempt, &adapter))
                        .catch_unwind()
                        .await;
                    match polled {
                        Ok(Ok(response)) => {
                            let _ = self.report_tx.send(WorkerReport::Polled {
                                task_id: self.task_id,
                                generation: self.generation,
                                response,
                            }).await;
                        }
                        Ok(Err(fatal)) => {
                            let _ = self.report_tx.send(WorkerReport::PollFailed {
                                task_id: self.task_id,
                                generation: self.generation,
                                reason: fatal.message,
                            }).await;
                            return;
                        }
                        Err(panic) => {
                            let _ = self.report_tx.send(WorkerReport::PollPanicked {
                                task_id: self.task_id,
                                generation: self.generation,
                                reason: panic_message(panic),
                            }).await;
                            return;
                        }
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => match cmd {
                    WorkerCmd::Pause => {
                        paused = true;
                        if self.report_tx.send(WorkerReport::Paused {
                            task_id: self.task_id,
                            generation: self.generation,
                        }).await.is_err() {
                            return ;
                        };
                    }
                    WorkerCmd::Resume => {}
                },
            }
        }
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
