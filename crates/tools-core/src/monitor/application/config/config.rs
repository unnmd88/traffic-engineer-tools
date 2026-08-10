use crate::monitor::application::config::QuerySnmpGet;

#[derive(Debug)]
pub struct AppConfig {
    pub groups: Vec<TaskGroupConfig>,
}

#[derive(Debug)]
pub struct TaskGroupConfig {
    pub name: String,
    pub tasks: Vec<TaskConfig>,
}

#[derive(Debug)]
pub struct TaskPollTimings {
    pub timeout_ms: u64,
    pub retries: u8,
    pub retry_delay_ms: u64,
}

#[derive(Debug)]
pub struct TaskConfig {
    pub name: String,
    pub poll_timings: TaskPollTimings,
    pub interval: u64,
    pub deep_history: u8,
    pub query: Query,
}

#[derive(Debug)]
pub enum Query {
    SnmpGet(QuerySnmpGet),
}
