use crate::monitor::Uid;

#[derive(Debug, Clone)]
pub struct Mapping {
    pub groups: Vec<GroupMapping>,
}

#[derive(Debug, Clone)]
pub struct GroupMapping {
    pub name: String,
    pub tasks: Vec<TaskMapping>,
}

#[derive(Debug, Clone)]
pub struct TaskMapping {
    pub name: String,
    pub uid: Uid,
}
