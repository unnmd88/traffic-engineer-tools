use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Payload, Pollable, SnmpError,
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

pub struct CustomReader {
    client: SnmpReadClient,
    oids_to_request: Vec<SnmpOid>,
    oid_names: Vec<Option<String>>,
}

impl CustomReader {
    pub fn new(client: SnmpReadClient, request: Vec<SnmpQueryItem>) -> Self {
        let mut oids_to_request: Vec<SnmpOid> = Vec::with_capacity(request.len());
        let mut oid_names: Vec<Option<String>> = Vec::with_capacity(request.len());

        for items in request {
            oids_to_request.push(items.oid);
            oid_names.push(items.name);
        }

        Self {
            client,
            oids_to_request,
            oid_names,
        }
    }
}

#[async_trait]
impl Pollable for CustomReader {
    type Output = SnmpGetResponse;

    async fn poll(&self) -> Result<Self::Output, PollError> {
        let payload = self
            .client
            .get_many(&self.oids_to_request)
            .await
            .map_err(|e| PollError::Other {
                message: e.to_string(),
            })?
            .into_iter()
            .zip(self.oid_names.iter())
            .map(|(varbind, name)| SnmpGetSample {
                oid_name: name.clone(),
                oid: varbind.oid.to_string(),
                raw_value: varbind.value.to_string(),
                value: None,
            })
            .collect();

        return Ok(SnmpGetResponse { samples: payload });
    }
}
