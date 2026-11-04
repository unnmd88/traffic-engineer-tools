use std::panic::AssertUnwindSafe;

use chrono::Utc;
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

use crate::monitor::{
    probe::{ProbeError, ProbeExecution, ProbeSpec},
    task::TaskId,
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
    Executed {
        task_id: TaskId,
        generation: Generation,
        execution: ProbeExecution,
    },
    ExecutionFailed {
        task_id: TaskId,
        generation: Generation,
        error: ProbeError,
    },
    ExecutionPanicked {
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
            | Self::Executed {
                task_id,
                generation,
                ..
            }
            | Self::ExecutionFailed {
                task_id,
                generation,
                ..
            }
            | Self::ExecutionPanicked {
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
    probe_spec: ProbeSpec,
    report_tx: mpsc::Sender<WorkerReport>,
    cmd_rx: mpsc::Receiver<WorkerCmd>,
}

impl Worker {
    pub fn new(
        task_id: TaskId,
        generation: Generation,
        probe_spec: ProbeSpec,
        report_tx: mpsc::Sender<WorkerReport>,
        cmd_rx: mpsc::Receiver<WorkerCmd>,
    ) -> Self {
        Self {
            task_id,
            generation,
            probe_spec,
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
        let build_timeout = self.probe_spec.build_timeout();

        let built = AssertUnwindSafe(tokio::time::timeout(build_timeout, self.probe_spec.build()))
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

        let mut probe = match built {
            Ok(Ok(probe)) => probe,
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

        let mut next_deadline = probe.next_deadline();
        if self
            .report_tx
            .send(WorkerReport::Built {
                task_id: self.task_id,
                generation: self.generation,
            })
            .await
            .is_err()
        {
            return;
        }

        let mut paused = false;

        loop {
            if paused {
                let cmd = tokio::select! {
                    () = self.report_tx.closed() => return,
                    cmd = self.cmd_rx.recv() => cmd,
                };
                match cmd {
                    Some(WorkerCmd::Resume) => {
                        paused = false;
                        next_deadline = probe.next_deadline();
                        if self
                            .report_tx
                            .send(WorkerReport::Resumed {
                                task_id: self.task_id,
                                generation: self.generation,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Some(WorkerCmd::Pause) => {}
                    None => return,
                }
                continue;
            }

            tokio::select! {
                biased;
                () = self.report_tx.closed() => return,
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(WorkerCmd::Pause) => {
                        paused = true;
                        if self.report_tx.send(WorkerReport::Paused {
                            task_id: self.task_id,
                            generation: self.generation,
                        }).await.is_err() {
                            return;
                        }
                    }
                    Some(WorkerCmd::Resume) => {}
                    None => return,
                },
                () = sleep_until(next_deadline) => {
                    let started_at = Instant::now();
                    let executed = AssertUnwindSafe(probe.do_work())
                        .catch_unwind()
                        .await;
                    match executed {
                        Ok(Ok(outcome)) => {
                            let execution = ProbeExecution {
                                finished_at: Utc::now(),
                                elapsed: started_at.elapsed(),
                                outcome,
                            };
                            next_deadline = probe.next_deadline();
                            if self.report_tx.send(WorkerReport::Executed {
                                task_id: self.task_id,
                                generation: self.generation,
                                execution,
                            }).await.is_err() {
                                return;
                            }
                        }
                        Ok(Err(error)) => {
                            let _ = self.report_tx.send(WorkerReport::ExecutionFailed {
                                task_id: self.task_id,
                                generation: self.generation,
                                error,
                            }).await;
                            return;
                        }
                        Err(panic) => {
                            let _ = self.report_tx.send(WorkerReport::ExecutionPanicked {
                                task_id: self.task_id,
                                generation: self.generation,
                                reason: panic_message(panic),
                            }).await;
                            return;
                        }
                    }
                }
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

#[cfg(test)]
mod tests {
    use tokio::{
        net::UdpSocket,
        task::{JoinHandle, yield_now},
        time::{Duration, advance, timeout},
    };

    use super::*;
    use crate::{
        monitor::probe::{ProbeOutcome, RawSnmpOidItem, SnmpGetSpec},
        polling::{AttemptConfig, Response},
    };

    async fn start_worker()
    -> (JoinHandle<()>, mpsc::Sender<WorkerCmd>, mpsc::Receiver<WorkerReport>, UdpSocket) {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request = SnmpGetSpec::from_raw(
            "127.0.0.1".to_string(),
            peer.local_addr().unwrap().port(),
            "public".to_string(),
            None,
            vec![RawSnmpOidItem {
                name: None,
                oid: "1.3.6.1.2.1.1.1.0".to_string(),
            }],
        )
        .unwrap();
        let attempt = AttemptConfig::try_new(Duration::from_secs(1), 0, Duration::ZERO).unwrap();
        let spec = ProbeSpec::snmp_get(request, Duration::from_secs(10), 0, attempt).unwrap();
        let (report_tx, mut reports) = mpsc::channel(8);
        let (commands, cmd_rx) = mpsc::channel(8);
        let worker = Worker::new(TaskId(1), Generation::first(), spec, report_tx, cmd_rx);
        let join = tokio::spawn(worker.run());
        assert!(matches!(reports.recv().await, Some(WorkerReport::Built { .. })));
        (join, commands, reports, peer)
    }

    #[tokio::test(start_paused = true)]
    async fn redundant_resume_does_not_postpone_execution() {
        let (join, commands, mut reports, _peer) = start_worker().await;
        for _ in 0..9 {
            advance(Duration::from_secs(1)).await;
            commands.send(WorkerCmd::Resume).await.unwrap();
            yield_now().await;
        }
        let report = timeout(Duration::from_secs(3), reports.recv())
            .await
            .unwrap()
            .unwrap();
        let WorkerReport::Executed { execution, .. } = report else {
            panic!("expected a completed scenario");
        };
        assert!(matches!(execution.outcome, ProbeOutcome::SnmpGet(Response::NoResponse { .. })));
        drop(commands);
        join.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn pause_waits_for_the_whole_execution() {
        let (join, commands, mut reports, peer) = start_worker().await;
        let mut packet = [0; 1024];
        peer.recv(&mut packet).await.unwrap();
        commands.send(WorkerCmd::Pause).await.unwrap();

        assert!(matches!(reports.recv().await, Some(WorkerReport::Executed { .. })));
        assert!(matches!(reports.recv().await, Some(WorkerReport::Paused { .. })));
        advance(Duration::from_secs(100)).await;
        yield_now().await;
        assert!(reports.try_recv().is_err());

        commands.send(WorkerCmd::Resume).await.unwrap();
        assert!(matches!(reports.recv().await, Some(WorkerReport::Resumed { .. })));
        assert!(matches!(reports.recv().await, Some(WorkerReport::Executed { .. })));
        drop(commands);
        join.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn closed_command_channel_ends_the_worker() {
        let (join, commands, _reports, _peer) = start_worker().await;
        drop(commands);
        timeout(Duration::from_secs(1), join)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn closed_report_channel_ends_the_worker() {
        let (join, _commands, reports, _peer) = start_worker().await;
        drop(reports);
        timeout(Duration::from_secs(1), join)
            .await
            .unwrap()
            .unwrap();
    }
}
