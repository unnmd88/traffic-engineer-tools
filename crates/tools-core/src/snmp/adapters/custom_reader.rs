use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Payload, Pollable, SnmpError, Updateble,
    domain::ascii::Ascii,
    error::{PollError, UpdateError},
    snmp::{
        SnmpGetQueryItem, SnmpReadClient,
        business_value::BusinessValue,
        oid::SnmpOid,
        oid_metadata::OidMetadata,
        oids_resolve::resolve_oids,
        parsers::OidValueParserFn,
        profiles::SnmpProfile,
        response::{SnmpGetResponse, SnmpGetSample},
        site_id,
    },
    utils::get_elapsed_as_u64,
};
use async_snmp::value;
use async_trait::async_trait;
use chrono::{Utc, naive};
use serde::de::IntoDeserializer;
use tokio::time::Instant;
use tracing::warn;

struct InnerQueryItem {
    name: Option<String>,
    parser: Option<OidValueParserFn>,
}

pub struct CustomReader {
    client: SnmpReadClient,
    profile: Option<SnmpProfile>,
    oids_to_request: Vec<SnmpOid>,
    query_items: Vec<InnerQueryItem>,
    request: Vec<SnmpGetQueryItem>,
}

impl CustomReader {
    pub async fn new(
        client: SnmpReadClient,
        request: Vec<SnmpGetQueryItem>,
        profile: Option<SnmpProfile>,
    ) -> Result<Self, SnmpError> {
        let capacity = request.len();

        let mut oids = Vec::with_capacity(capacity);
        let mut query_items = Vec::with_capacity(capacity);

        for item in request.iter() {
            oids.push(item.oid.clone());

            let metadata: Option<OidMetadata> =
                profile.as_ref().and_then(|p| p.metadata(&item.oid));

            let parser = item
                .business_value_parser
                .or_else(|| metadata.as_ref().and_then(|m| m.parser));
            let name = item
                .name
                .clone()
                .or_else(|| metadata.as_ref().map(|m| m.name.to_string()));
            query_items.push(InnerQueryItem { name, parser });
        }

        // Resolved oids. If profile has SCN - create new oids with SCN.
        let resolved_oids = resolve_oids(&client, profile.as_ref(), &oids).await?;

        Ok(Self {
            client,
            profile,
            oids_to_request: resolved_oids,
            query_items,
            request,
        })
    }
}

#[async_trait]
impl Updateble for CustomReader {
    type Instance = Self;

    async fn update(self) -> Result<Self::Instance, UpdateError> {
        let config = self.client.config().clone();
        let new_client = SnmpReadClient::new(config)
            .await
            .map_err(|e| UpdateError::Adapter {
                message: format!("Fail to create snmp-read client: {e}"),
            })?;
        Ok(Self::new(new_client, self.request, self.profile)
            .await
            .map_err(|e| UpdateError::Adapter {
                message: format!("Fail update: {e}"),
            })?)
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
            .zip(&self.query_items)
            .map(|(vb, query_item)| {
                let parsed_value = match query_item.parser {
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
