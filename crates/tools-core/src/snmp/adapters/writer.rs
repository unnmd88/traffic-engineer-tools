use async_trait::async_trait;

use crate::{
    polling::{AttemptError, Pollable},
    snmp::{SnmpClient, SnmpError, SnmpSetItem, oid::SnmpOid, value::SnmpValue},
};

/// Адаптер записи: держит зарезолвленные (oid, value) и шлёт их атомарным
/// multi-varbind SET. Резолвинг (SCN) уже сделан на этапе `snmp::resolve`.
pub struct SnmpWriter {
    client: SnmpClient,
    items: Vec<SnmpSetItem>,
}

impl SnmpWriter {
    /// Чистая сборка из уже зарезолвленных элементов (см. `snmp::resolve`).
    pub fn new(client: SnmpClient, items: Vec<SnmpSetItem>) -> Self {
        Self { client, items }
    }
}

#[async_trait]
impl Pollable for SnmpWriter {
    type Output = ();

    async fn poll(&self) -> Result<Self::Output, AttemptError> {
        let sets: Vec<(SnmpOid, SnmpValue)> = self
            .items
            .iter()
            .map(|i| (i.oid.clone(), i.value.clone()))
            .collect();

        self.client
            .set_many(&sets)
            .await
            .map_err(classify_snmp_error)?;

        Ok(())
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
