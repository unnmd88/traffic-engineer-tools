use core::ascii;

use crate::{
    SnmpError,
    domain::ascii::Ascii,
    snmp::{
        SnmpGetQueryItem, SnmpReadClient, oid::SnmpOid, profiles::SnmpProfile,
        registry::scn_required,
    },
};

#[derive(Clone)]
pub struct OidResolver {
    profile: Option<SnmpProfile>,
    client: SnmpReadClient,
}

impl OidResolver {
    pub fn new(client: SnmpReadClient) -> Self {
        Self {
            client,
            profile: None,
        }
    }

    pub fn with_profile(self, profile: SnmpProfile) -> Self {
        Self {
            client: self.client,
            profile: Some(profile),
        }
    }

    pub async fn resolve(&self, oids: &[SnmpOid]) -> Result<Vec<SnmpOid>, SnmpError> {
        resolve_oids(&self.client, self.profile.as_ref(), oids).await
    }
}

pub async fn resolve_oids(
    client: &SnmpReadClient,
    profile: Option<&SnmpProfile>,
    to_resolve: &[SnmpOid],
) -> Result<Vec<SnmpOid>, SnmpError> {
    let p = match profile {
        Some(p) => p,
        None => return Ok(to_resolve.to_vec()),
    };
    let site_id = match p.get_site_id_if_required(client).await? {
        Some(id) => id,
        None => return Ok(to_resolve.to_vec()),
    };

    let scn = Ascii::from_bytes(&site_id)
        .map_err(|e| SnmpError::ConvertScn(e.to_string()))?
        .to_scn();

    let mut result = Vec::with_capacity(to_resolve.len());

    for item in to_resolve {
        let oid = if scn_required(&item) {
            SnmpOid::parse(&format!("{}{}", item.to_string(), scn)).map_err(|e| {
                tracing::error!(target: "resolve_oids", "Bug: {}", e);
                SnmpError::ResolveOid(e.to_string())
            })?
        } else {
            item.clone()
        };
        result.push(oid);
    }

    Ok(result)
}
