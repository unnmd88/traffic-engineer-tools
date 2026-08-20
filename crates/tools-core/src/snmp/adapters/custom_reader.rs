use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Payload, Pollable, SnmpError,
    error::PollError,
    snmp::{
        SnmpQueryItem, SnmpReadClient,
        parsers::OidValueParserFn,
        primitives::{SnmpOid, SnmpRawValue},
        response::{SnmpGetResponse, SnmpGetSample},
    },
    utils::get_elapsed_as_u64,
};
use async_snmp::value;
use async_trait::async_trait;
use chrono::{Utc, naive};
use serde::de::IntoDeserializer;
use tokio::time::Instant;
use tracing::warn;

pub struct CustomReader {
    client: SnmpReadClient,
    oids_to_request: Vec<SnmpOid>,
    request: Vec<SnmpQueryItem>,
}

impl CustomReader {
    pub fn new(client: SnmpReadClient, request: Vec<SnmpQueryItem>) -> Self {
        let oids_to_request = request.iter().map(|i| i.oid.clone()).collect();
        Self {
            client,
            oids_to_request,
            request,
        }
    }
}

#[async_trait]
impl Pollable for CustomReader {
    type Output = SnmpGetResponse;

    async fn poll(&self) -> Result<Self::Output, PollError> {
        let samples = self
            .client
            .get_many(&self.oids_to_request)
            .await
            .map_err(|e| PollError::Other {
                message: e.to_string(),
            })?
            .into_iter()
            .zip(self.request.iter())
            .map(|(vb, query_item)| {
                let parsed_value = match query_item.parser {
                    Some(parser) => {
                        match parser(&vb.value) {
                            Ok(val) => Some(val),
                            Err(e) => {
                                tracing::error!(target: "CustomReader", value = ?&vb.value, "{e}");
                                Some("parse error".to_string()) // 👈 оборачиваем в Some
                            }
                        }
                    }
                    None => None,
                };
                SnmpGetSample {
                    oid_name: query_item.name.clone(),
                    oid: vb.oid.to_string(),
                    raw_value: vb.value.to_string(),
                    value: parsed_value,
                }
            })
            .collect();

        return Ok(SnmpGetResponse { samples });
    }
}
