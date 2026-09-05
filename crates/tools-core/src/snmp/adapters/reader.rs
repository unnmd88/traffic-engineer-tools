use async_trait::async_trait;

use crate::{
    SnmpError,
    error::PollError,
    polling::Pollable,
    snmp::{
        SnmpGetQueryItem, SnmpClient,
        business_value::BusinessValue,
        oid::SnmpOid,
        parsers::OidValueParserFn,
        profiles::SnmpProfile,
        response::{SnmpGetResponse, SnmpGetSample},
    },
};

struct ResolvedItem {
    oid: SnmpOid,
    name: Option<String>,
    parser: Option<OidValueParserFn>,
}

pub struct SnmpReader {
    client: SnmpClient,
    items: Vec<ResolvedItem>,
}

impl SnmpReader {
    pub async fn new(
        client: SnmpClient,
        request: Vec<SnmpGetQueryItem>,
        profile: Option<SnmpProfile>,
    ) -> Result<Self, SnmpError> {
        let mut items = Vec::with_capacity(request.len());

        for item in request {
            let metadata = profile
                .as_ref()
                .and_then(|p| p.get_metadata_by_oid(&item.oid));

            let parser = item
                .business_value_parser
                .or_else(|| metadata.as_ref().and_then(|m| m.parser));
            let name = item
                .name
                .or_else(|| metadata.as_ref().map(|m| m.name.to_string()));

            items.push(ResolvedItem {
                oid: item.oid,
                name,
                parser,
            });
        }

        // SCN-резолюция: если профиль требует, дополняем OID идентификатором контроллера.
        let oids: Vec<SnmpOid> = items.iter().map(|i| i.oid.clone()).collect();
        let resolved_oids = match &profile {
            Some(profile) => profile.resolve_oids(&client, &oids).await?,
            None => oids,
        };
        for (item, oid) in items.iter_mut().zip(resolved_oids) {
            item.oid = oid;
        }

        Ok(Self { client, items })
    }
}

#[async_trait]
impl Pollable for SnmpReader {
    type Output = SnmpGetResponse;

    async fn poll(&self) -> Result<Self::Output, PollError> {
        let oids: Vec<SnmpOid> = self.items.iter().map(|i| i.oid.clone()).collect();

        let samples = self
            .client
            .get_many(&oids)
            .await
            .map_err(|e| PollError::Other {
                message: e.to_string(),
            })?
            .into_iter()
            .zip(&self.items)
            .map(|(vb, item)| {
                let parsed_value = item.parser.map(|parser| match parser(&vb.value) {
                    Ok(val) => val,
                    Err(e) => {
                        tracing::error!(target: "snmp_reader", value = ?&vb.value, "{e}");
                        BusinessValue::Text("parse error".to_string())
                    }
                });

                SnmpGetSample {
                    oid_name: item.name.clone(),
                    oid: vb.oid,
                    raw_value: vb.value,
                    value: parsed_value,
                }
            })
            .collect();

        Ok(SnmpGetResponse { samples })
    }
}
