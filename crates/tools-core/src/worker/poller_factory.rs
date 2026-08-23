use std::net::IpAddr;
use tokio::time::Duration;

use crate::{
    Error,
    polling::{PollConfig, Poller},
    snmp::{
        SnmpGetQueryItem, SnmpGetResponse, SnmpReadClient, adapters::CustomReader,
        community::Community,
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
        oids_to_request: Vec<SnmpGetQueryItem>,
    ) -> Result<Poller<CustomReader>, Error> {
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
        let adapter = CustomReader::new(client, oids_to_request);
        Ok(Poller::new(adapter, self.poll_config.clone()))
    }

    pub fn snmp_get_use_case_with_client(
        &self,
        client: SnmpReadClient,
        oids_to_request: Vec<SnmpGetQueryItem>,
    ) -> Poller<CustomReader> {
        let adapter = CustomReader::new(client, oids_to_request);
        Poller::new(adapter, self.poll_config.clone())
    }
}
