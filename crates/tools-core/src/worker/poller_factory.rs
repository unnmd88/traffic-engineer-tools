use std::net::IpAddr;
use tokio::time::Duration;

use crate::{
    Error,
    polling::{PollConfig, Poller},
    snmp::{
        SnmpGetResponse, SnmpQueryItem, SnmpReadClient, adapters::GenericCustomReader,
        primitives::Community,
    },
};

pub struct PollerFactory {
    poll_config: PollConfig,
}

impl PollerFactory {
    pub fn new(poll_config: PollConfig) -> Self {
        Self { poll_config }
    }

    pub async fn snmp_get_use_case(
        &self,
        target: IpAddr,
        port: u16,
        community: Community,
        oids_to_request: Vec<SnmpQueryItem>,
    ) -> Result<Poller<GenericCustomReader>, Error> {
        let client = SnmpReadClient::new(
            target,
            port,
            community,
            self.poll_config
                .timeout
                .saturating_add(Duration::from_millis(1000)),
            0,
            Duration::from_secs(1),
        )
        .await?;
        let adapter = GenericCustomReader::new(client, oids_to_request);
        Ok(Poller::new(adapter, self.poll_config.clone()))
    }
}
