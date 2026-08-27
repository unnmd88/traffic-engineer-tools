use crate::{
    Pollable,
    error::PollError,
    poll_response::Response,
    worker::{Metrics, WorkerId, WorkerState},
};

#[derive(Clone, Debug)]
pub struct PollEvent<P: Pollable> {
    pub metrics: Metrics,
    pub state: WorkerState,
    pub result: Result<Response<P::Output>, PollError>,
}
