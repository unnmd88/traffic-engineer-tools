use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Payload, Pollable, SnmpError,
    error::PollError,
    snmp::{
        SnmpGetQueryItem, SnmpReadClient,
        business_value::BusinessValue,
        oid::SnmpOid,
        parsers::OidValueParserFn,
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
    request: Vec<SnmpGetQueryItem>,
}

impl CustomReader {
    pub fn new(client: SnmpReadClient, request: Vec<SnmpGetQueryItem>) -> Self {
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
                let parsed_value = match query_item.business_value_parser {
                    Some(parser) => match parser(&vb.value) {
                        Ok(val) => Some(val),
                        Err(e) => {
                            tracing::error!(target: "CustomReader", value = ?&vb.value, "{e}");
                            Some(BusinessValue::Text("parse error".to_string()))
                        }
                    },
                    None => None,
                };
                SnmpGetSample {
                    oid_name: query_item.name.clone(),
                    oid: vb.oid,
                    raw_value: vb.value,
                    value: parsed_value,
                }
            })
            .collect();

        return Ok(SnmpGetResponse { samples });
    }
}
