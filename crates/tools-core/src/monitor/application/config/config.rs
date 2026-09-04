use crate::monitor::{application::config::QuerySnmpGet, task::TaskPollConfig};

#[derive(Debug)]
pub struct AppConfig {
    pub tasks: Vec<TaskConfigDto>,
}

#[derive(Debug)]
pub struct AttemptPollTimingsDto {
    pub timeout_ms: u64,
    pub retries: u8,
    pub retry_delay_ms: u64,
}

#[derive(Debug)]
pub struct TaskConfigDto {
    pub name: String,
    pub attempt_timings: AttemptPollTimingsDto,
    pub interval_ms: u64,
    pub limit: u64,
    pub deep_history: u8,
    pub query: Query,
}

#[derive(Debug)]
pub enum Query {
    SnmpGet(QuerySnmpGet),
}
