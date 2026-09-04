use crate::{PollErrorContext, polling::Response, snmp::SnmpGetResponse};

#[derive(Clone, Debug)]
pub enum PollResult {
    Initial,
    NoResponse(Vec<PollErrorContext>),
    Fail { message: String },
    SnmpGet(Response<SnmpGetResponse>),
    // остальные use-cases
}

impl From<Response<SnmpGetResponse>> for PollResult {
    fn from(response: Response<SnmpGetResponse>) -> Self {
        PollResult::SnmpGet(response)
    }
}

// Остальные реализации для каждого use-case для PollResult
