use crate::{
    Pollable,
    error::PollError,
    poll_response::Response,
    worker::{Metrics, WorkerId, WorkerState},
};

pub enum WorkerEvent<A: Pollable> {
    PollSuccess {
        worker_id: WorkerId,
        metrics: Metrics,
        state: WorkerState,
        payload: Response<A::Output>,
    },
    PollError {
        worker_id: WorkerId,
        metrics: Metrics,
        state: WorkerState,
        error: PollError,
    },
}
