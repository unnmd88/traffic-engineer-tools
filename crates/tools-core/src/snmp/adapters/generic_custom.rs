use std::net::SocketAddr;

use crate::{
    Error, Payload, Pollable, SnmpError,
    error::PollError,
    snmp::{
        SnmpQueryItem, SnmpReadClient,
        primitives::SnmpOid,
        response::{SnmpGetResponse, SnmpGetSample},
    },
    utils::get_elapsed_as_u64,
};
use async_trait::async_trait;
use chrono::{Utc, naive};
use serde::de::IntoDeserializer;
use tokio::time::Instant;

pub struct GenericCustomReader {
    //target: SocketAddr,
    //name: Option<String>,
    client: SnmpReadClient,
    oids_to_request: Vec<SnmpOid>,
    oid_names: Vec<Option<String>>,
}

impl GenericCustomReader {
    pub fn new(client: SnmpReadClient, request: Vec<SnmpQueryItem>) -> Self {
        let mut oids_to_request: Vec<SnmpOid> = Vec::with_capacity(request.len());
        let mut oid_names: Vec<Option<String>> = Vec::with_capacity(request.len());

        for items in request {
            oids_to_request.push(items.oid);
            oid_names.push(items.name);
        }

        Self {
            //name,
            //target: client.socket_addr(),
            client,
            oids_to_request,
            oid_names,
        }
    }
}

#[async_trait]
impl Pollable for GenericCustomReader {
    //type Output = Payload<SnmpGetResponse>;
    type Output = SnmpGetResponse;

    /*
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn target(&self) -> String {
        self.target.to_string()
    }
    */

    async fn poll(&self) -> Result<Self::Output, Error> {
        let payload = self
            .client
            .get_many(&self.oids_to_request)
            .await?
            .into_iter()
            .zip(self.oid_names.iter())
            .map(|(varbind, name)| SnmpGetSample {
                oid_name: name.clone(),
                oid: varbind.oid.to_string(),
                raw_value: varbind.value.to_string(),
            })
            .collect();

        return Ok(SnmpGetResponse { samples: payload });
    }
}
