use crate::snmp::{SnmpClient, SnmpError, SnmpVarbind, oid::SnmpOid};

/// Полный инстанс utcReplySiteID (колонка с фикс. индексом 0 для Potok).
const UTC_REPLY_SITE_ID_POTOK_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.2.0";

pub async fn fetch_site_id_potok_ug405(client: &SnmpClient) -> Result<SnmpVarbind, SnmpError> {
    let oid = SnmpOid::parse(UTC_REPLY_SITE_ID_POTOK_OID)?;
    client.get(&oid).await
}
