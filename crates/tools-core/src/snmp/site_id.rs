use crate::{
    SnmpError,
    snmp::{SnmpReadClient, oid::SnmpOid, registry::oids::UTC_REPLY_SITE_ID_POTOK_OID},
};

pub async fn fetch_site_id_potok_ug405(client: &SnmpReadClient) -> Result<Vec<u8>, SnmpError> {
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
