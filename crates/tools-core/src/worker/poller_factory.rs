use std::net::IpAddr;
use tokio::time::Duration;

use crate::{
    Error,
    polling::PollConfig,
    snmp::{
        SnmpGetQueryItem, SnmpGetResponse, SnmpReadClient, SnmpReadClientConfig,
        adapters::CustomReader, community::Community,
    },
};

pub struct PollerFactory {
    poll_config: PollConfig,
}

impl PollerFactory {
    pub fn new(poll_config: PollConfig) -> Self {
        Self { poll_config }
    }
}
