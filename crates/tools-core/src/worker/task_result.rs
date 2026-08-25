use crate::{PollErrorContext, poll_response::Response, snmp::SnmpGetResponse};

#[derive(Clone, Debug)]
pub enum TaskResult {
    Initial,
    SnmpGet(Response<SnmpGetResponse>),
    NoResponse(Vec<PollErrorContext>),
    Fail { message: String },
}

impl From<Response<SnmpGetResponse>> for TaskResult {
    fn from(response: Response<SnmpGetResponse>) -> Self {
        TaskResult::SnmpGet(response)
    }
}
