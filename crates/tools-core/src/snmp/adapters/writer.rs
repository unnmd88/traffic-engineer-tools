use async_trait::async_trait;

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{
        SnmpClient, SnmpError, SnmpSetItem,
        oid::SnmpOid,
        profiles::SnmpProfile,
        response::{SnmpSetResponse, SnmpSetSample},
        value::SnmpValue,
    },
};

/// Адаптер записи: держит зарезолвленные (oid, value) и шлёт их атомарным
/// multi-varbind SET. Весь резолвинг (OID + кодирование value) уже сделан
/// на границе в `SnmpSetQuery::from_raw`.
pub struct SnmpWriter {
    client: SnmpClient,
    items: Vec<SnmpSetItem>,
}

impl SnmpWriter {
    pub async fn new(
        client: SnmpClient,
        mut items: Vec<SnmpSetItem>,
        profile: Option<SnmpProfile>,
    ) -> Result<Self, SnmpError> {
        // SCN-резолюция — единственное, что требует живого клиента.
        let oids: Vec<SnmpOid> = items.iter().map(|i| i.oid.clone()).collect();
        let resolved = match &profile {
            Some(profile) => profile.resolve_oids(&client, &oids).await?,
            None => oids,
        };
        for (item, oid) in items.iter_mut().zip(resolved) {
            item.oid = oid;
        }

        Ok(Self { client, items })
    }
}

#[async_trait]
impl Pollable for SnmpWriter {
    type Output = SnmpSetResponse;

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let sets: Vec<(SnmpOid, SnmpValue)> = self
            .items
            .iter()
            .map(|i| (i.oid.clone(), i.value.clone()))
            .collect();

        let varbinds = self
            .client
            .set_many(&sets)
            .await
            .map_err(classify_snmp_error)?;

        Ok(SnmpSetResponse {
            samples: varbinds
                .into_iter()
                .zip(&self.items)
                .map(|(vb, item)| SnmpSetSample {
                    oid_name: item.name.clone(),
                    oid: vb.oid,
                    value: vb.value,
                })
                .collect(),
        })
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
