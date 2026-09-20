use async_trait::async_trait;

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{
        SnmpClient, SnmpError, SnmpGetItem,
        business_value::BusinessValue,
        oid::SnmpOid,
        response::SnmpGetSample,
    },
};

pub struct SnmpReader {
    client: SnmpClient,
    items: Vec<SnmpGetItem>,
}

impl SnmpReader {
    /// Чистая сборка из уже зарезолвленных элементов (см. `snmp::resolve`).
    pub fn new(client: SnmpClient, items: Vec<SnmpGetItem>) -> Self {
        Self { client, items }
    }
}

#[async_trait]
impl Pollable for SnmpReader {
    type Output = Vec<SnmpGetSample>;

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let oids: Vec<SnmpOid> = self.items.iter().map(|i| i.oid.clone()).collect();

        let samples = self
            .client
            .get_many(&oids)
            .await
            .map_err(classify_snmp_error)?
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

        Ok(samples)
    }
}

fn classify_snmp_error(e: SnmpError) -> AttemptError {
    match e {
        SnmpError::Network { .. } | SnmpError::Timeout { .. } => {
            AttemptError::Transient(e.to_string())
        }
        _ => AttemptError::Fatal(e.to_string()),
    }
}
