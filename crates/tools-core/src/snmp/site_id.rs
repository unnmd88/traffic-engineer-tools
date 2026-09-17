use crate::snmp::{SnmpError, SnmpClient, oid::SnmpOid};

/// Полный инстанс utcReplySiteID (колонка с фикс. индексом 0 для Potok).
const UTC_REPLY_SITE_ID_POTOK_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.2.0";

pub async fn fetch_site_id_potok_ug405(client: &SnmpClient) -> Result<Vec<u8>, SnmpError> {
    let oid = SnmpOid::parse(UTC_REPLY_SITE_ID_POTOK_OID)?;
    let varbind = client.get(&oid).await?;

    varbind
        .value
        .as_bytes()
        .map(|b| b.to_vec())
        .ok_or_else(|| SnmpError::UnexpectedValueType {
            expected: "OctetString".to_string(),
            actual: varbind.value.as_string(),
        })
}
