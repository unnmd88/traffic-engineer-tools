use crate::{PollErrorContext, monitor::application::UseCaseOutput, polling::Response};

#[derive(Clone, Debug)]
pub enum PollResult {
    Initial,
    NoResponse(Vec<PollErrorContext>),
    Fail { message: String },
    Success(Response<UseCaseOutput>),
}
impl From<Response<UseCaseOutput>> for PollResult {
    fn from(r: Response<UseCaseOutput>) -> Self {
        Self::Success(r)
    }
}
