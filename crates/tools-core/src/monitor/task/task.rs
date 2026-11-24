use serde::{Deserialize, Serialize};

use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use derive_more::{Constructor, Display};

use crate::monitor::{
    scenario::ScenarioExecution,
    task::{ExecutionMetrics, RunLimit, TaskConfig},
};

/// The task lifecycle, independent of operation results and scenario progress messages.
#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Registered without an active scenario.
    Idle,
    /// Preparing a scenario before its regular operations can start.
    Preparing,
    /// Executing a scenario, including waits for its next scheduled operation.
    Active,
    /// Execution is paused.
    Paused,
    /// Execution was stopped.
    Stopped,
    /// The execution limit or a scenario completion condition was reached.
    Completed,
    /// Waiting to prepare a new scenario after a failure.
    Restarting,
}

#[derive(
    Clone,
    Debug,
    Copy,
    Display,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Constructor,
    Serialize,
    Deserialize,
)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug, Copy, Display, PartialEq, Eq, Constructor, Serialize, Deserialize)]
pub struct SpecRevision(pub u64);

impl SpecRevision {
    pub fn next(self) -> Self {
        Self::new(self.0 + 1)
    }
}

#[derive(Clone, Debug)]
pub(in crate::monitor) struct Task {
    id: TaskId,
    config: TaskConfig,
    revision: SpecRevision,
    config_updated_at: DateTime<Utc>,
    last_result: Option<ScenarioExecution>,
    history: VecDeque<ScenarioExecution>,
    metrics: ExecutionMetrics,
    run_limit: Option<RunLimit>,
    completed_runs: u64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(id: TaskId, config: TaskConfig) -> Self {
        let dt = Utc::now();
        let deep_history = config.deep_history();
        Self {
            id,
            config,
            revision: SpecRevision::new(1),
            config_updated_at: dt,
            last_result: None,
            metrics: ExecutionMetrics::default(),
            run_limit: None,
            completed_runs: 0,
            history: VecDeque::with_capacity(usize::from(deep_history)),
            created_at: dt,
            updated_at: dt,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub const fn config(&self) -> &TaskConfig {
        &self.config
    }

    pub const fn revision(&self) -> SpecRevision {
        self.revision
    }

    pub const fn config_updated_at(&self) -> &DateTime<Utc> {
        &self.config_updated_at
    }

    pub fn last_result(&self) -> Option<&ScenarioExecution> {
        self.last_result.as_ref()
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics
    }

    pub const fn completed_runs(&self) -> u64 {
        self.completed_runs
    }

    pub const fn run_limit(&self) -> Option<RunLimit> {
        self.run_limit
    }

    pub fn record_completed_run(&mut self) {
        self.completed_runs += 1;
    }

    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }

    pub fn history(&self) -> &VecDeque<ScenarioExecution> {
        &self.history
    }

    pub fn apply_execution(&mut self, result: ScenarioExecution) {
        if let Some(detail) = result.report.detail() {
            self.metrics.record(detail.status(), result.elapsed());
        }
        self.store_execution(result);
    }

    /// Stores a result for observation without updating counters.
    pub fn store_execution(&mut self, result: ScenarioExecution) {
        self.updated_at = result.finished_at();
        let depth = usize::from(self.config.deep_history());
        if let Some(previous) = self.last_result.replace(result)
            && depth > 0
        {
            if self.history.len() >= depth {
                self.history.pop_back();
            }
            self.history.push_front(previous);
        }
    }

    pub fn update_config(&mut self, config: TaskConfig) {
        let revision = self.revision.next();
        let now = Utc::now();
        self.history.truncate(usize::from(config.deep_history()));
        self.config = config;
        self.revision = revision;
        self.config_updated_at = now;
        self.updated_at = now;
    }

    /// Starts accounting for a new execution with its own limit.
    pub fn begin_run(&mut self, limit: RunLimit) {
        self.run_limit = Some(limit);
        self.metrics = ExecutionMetrics::default();
        self.completed_runs = 0;
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::OperationReport;
    use crate::{
        monitor::scenario::{PollSchedule, ScenarioOutcome, ScenarioSpec, SnmpGetScenarioSpec},
        polling::{AttemptConfig, AttemptOutcome, AttemptReport, PollOutcome, PollReport},
        snmp::SnmpGetSpec,
    };
    use tokio::time::Duration;

    fn config(name: &str) -> TaskConfig {
        config_with_history(name, 3)
    }

    fn config_with_history(name: &str, deep_history: u8) -> TaskConfig {
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt =
            AttemptConfig::try_new(Duration::from_millis(100), 2, Duration::ZERO).unwrap();
        let scenario = ScenarioSpec::SnmpGet(
            SnmpGetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(1),
                },
                attempt,
            )
            .unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, deep_history).unwrap()
    }

    fn failed_execution(finished_at: DateTime<Utc>) -> ScenarioExecution {
        let elapsed = Duration::from_millis(300);
        ScenarioExecution::new(
            crate::monitor::OperationContext::new(
                crate::monitor::OperationId::new(0),
                crate::monitor::Generation::first(),
                SpecRevision::new(1),
            ),
            OperationReport::from_poll(
                PollReport {
                    started_at: finished_at - elapsed,
                    finished_at,
                    elapsed,
                    config: AttemptConfig::try_new(Duration::from_millis(100), 2, Duration::ZERO)
                        .unwrap(),
                    attempts: (0..3)
                        .map(|index| AttemptReport {
                            started_after: Duration::from_millis(index * 100),
                            elapsed: Duration::from_millis(100),
                            outcome: AttemptOutcome::TimedOut,
                        })
                        .collect(),
                    outcome: PollOutcome::RetryLimitReached,
                },
                ScenarioOutcome::SnmpGet,
            ),
        )
    }

    #[test]
    fn execution_and_restart_preserve_configuration_metadata() {
        let mut task = Task::new(TaskId(1), config("first"));
        let config_updated_at = *task.config_updated_at();
        let finished_at = config_updated_at + chrono::Duration::seconds(1);

        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.config_updated_at(), task.created_at());
        task.apply_execution(failed_execution(finished_at));

        assert_eq!(task.updated_at(), &finished_at);
        assert_eq!(task.config_updated_at(), &config_updated_at);
        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.metrics().executions, 1);
        assert_eq!(task.metrics().failed, 1);

        task.begin_run(RunLimit::Unlimited);
        assert_eq!(task.config_updated_at(), &config_updated_at);
        assert_eq!(task.revision(), SpecRevision::new(1));
        assert_eq!(task.metrics(), ExecutionMetrics::default());
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at);
    }

    #[test]
    fn run_count_is_independent_of_operation_metrics() {
        let mut task = Task::new(TaskId(1), config("first"));
        let started_at = *task.created_at();
        for seconds in 1..=3 {
            task.apply_execution(failed_execution(started_at + chrono::Duration::seconds(seconds)));
        }
        assert_eq!(task.metrics().executions, 3);
        assert_eq!(task.completed_runs(), 0);

        task.record_completed_run();
        assert_eq!(task.completed_runs(), 1);
        assert_eq!(task.metrics().executions, 3);

        task.update_config(config("second"));
        assert_eq!(task.completed_runs(), 1);

        task.begin_run(RunLimit::Unlimited);
        assert_eq!(task.completed_runs(), 0);
    }

    #[test]
    fn config_update_advances_revision_and_preserves_execution_state() {
        let mut task = Task::new(TaskId(1), config("first"));
        let created_at = *task.created_at();
        let finished_at = created_at + chrono::Duration::seconds(1);
        task.apply_execution(failed_execution(finished_at));
        let metrics = task.metrics();
        let before_update = Utc::now();

        task.update_config(config("updated"));

        assert_eq!(task.config().name(), "updated");
        assert_eq!(task.revision(), SpecRevision::new(2));
        assert!(*task.config_updated_at() >= before_update);
        assert!(*task.config_updated_at() <= Utc::now());
        assert_eq!(task.config_updated_at(), task.updated_at());
        assert_eq!(task.created_at(), &created_at);
        assert_eq!(task.metrics(), metrics);
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at);

        task.update_config(config("third"));
        assert_eq!(task.config().name(), "third");
        assert_eq!(task.revision(), SpecRevision::new(3));
    }

    #[test]
    fn config_update_resizes_history_and_keeps_the_newest_previous_results() {
        let mut task = Task::new(TaskId(1), config("task"));
        let started_at = *task.created_at();
        let finished_at = |seconds| started_at + chrono::Duration::seconds(seconds);
        let history_times = |task: &Task| {
            task.history()
                .iter()
                .map(ScenarioExecution::finished_at)
                .collect::<Vec<_>>()
        };
        for seconds in 1..=5 {
            task.apply_execution(failed_execution(finished_at(seconds)));
        }
        let metrics = task.metrics();

        task.update_config(config_with_history("task", 1));

        assert_eq!(task.config().deep_history(), 1);
        assert_eq!(history_times(&task), vec![finished_at(4)]);
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at(5));
        assert_eq!(task.metrics(), metrics);

        task.update_config(config_with_history("task", 3));

        assert_eq!(history_times(&task), vec![finished_at(4)]);
        for seconds in 6..=8 {
            task.apply_execution(failed_execution(finished_at(seconds)));
        }
        let expected_history = vec![finished_at(7), finished_at(6), finished_at(5)];
        assert_eq!(history_times(&task), expected_history);

        task.update_config(config_with_history("renamed", 3));

        assert_eq!(history_times(&task), expected_history);
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at(8));
    }

    #[test]
    fn config_update_disables_and_reenables_history_without_dropping_the_last_result() {
        let mut task = Task::new(TaskId(1), config("task"));
        let started_at = *task.created_at();
        let finished_at = |seconds| started_at + chrono::Duration::seconds(seconds);
        for seconds in 1..=3 {
            task.apply_execution(failed_execution(finished_at(seconds)));
        }

        task.update_config(config_with_history("task", 0));

        assert!(task.history().is_empty());
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at(3));
        task.apply_execution(failed_execution(finished_at(4)));
        assert!(task.history().is_empty());
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at(4));

        task.update_config(config_with_history("task", 2));

        assert!(task.history().is_empty());
        for seconds in 5..=7 {
            task.apply_execution(failed_execution(finished_at(seconds)));
        }
        let retained = task.history();
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].finished_at(), finished_at(6));
        assert_eq!(retained[1].finished_at(), finished_at(5));
        assert_eq!(task.last_result().unwrap().finished_at(), finished_at(7));
    }
}
