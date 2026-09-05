use crate::{monitor::task::UseCaseQuery, polling::PollConfig};

// Спека задачи — валидированное декларативное описание (Spec).
#[derive(Clone, Debug)]
pub struct TaskSpec {
    pub name: String,
    pub query: UseCaseQuery,
    pub poll_config: PollConfig,
    pub deep_history: u8,
}
