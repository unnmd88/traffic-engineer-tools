//! Резолюция запроса в финальный план опроса.
//!
//! [`Resolver`] — единственная точка резолюции: держит клиент для чтения
//! (SCN-фетч) и профиль, отвечает за обогащение метаданными и дополнение OID
//! идентификатором контроллера (SCN).
//!
//! Контракт:
//! - без профиля — библиотека ничего не резолвит (raw-режим);
//! - с профилем — библиотека отвечает за резолюцию: если OID требует SCN,
//!   а получить его не удалось, резолюция завершается ошибкой (никогда не
//!   опрашиваем «сырой» OID, когда нужен SCN).

use crate::{
    ascii::Ascii,
    snmp::{
        SnmpClient, SnmpError, SnmpGetQueryItem, SnmpSetItem, oid::SnmpOid, oid_metadata::OidKind,
        parsers::OidValueParserFn, profiles::SnmpProfile, site_id::fetch_site_id_potok_ug405,
    },
};

#[derive(Debug, Clone)]
pub struct ResolvedItem {
    pub oid: SnmpOid,
    pub name: Option<String>,
    pub parser: Option<OidValueParserFn>,
}

pub struct Resolver {
    client: SnmpClient,
    profile: Option<SnmpProfile>,
}

impl Resolver {
    pub fn new(client: SnmpClient, profile: Option<SnmpProfile>) -> Self {
        Self { client, profile }
    }

    pub fn profile(&self) -> Option<SnmpProfile> {
        self.profile
    }

    pub async fn fetch_scn(&self) -> Result<Option<Ascii>, SnmpError> {
        match self.profile {
            Some(SnmpProfile::PotokUg405) => {
                let bytes = fetch_site_id_potok_ug405(&self.client).await?;
                let ascii =
                    Ascii::from_bytes(&bytes).map_err(|e| SnmpError::ConvertScn(e.to_string()))?;
                Ok(Some(ascii))
            }
            _ => Ok(None),
        }
    }

    /// Достраивает OID до полного инстанса по `OidKind` из реестра профиля:
    /// скаляры получают ".0", SCN-колонки — индекс контроллера.
    pub async fn resolve_oids(&self, to_resolve: &[SnmpOid]) -> Result<Vec<SnmpOid>, SnmpError> {
        let Some(profile) = self.profile else {
            return Ok(to_resolve.to_vec());
        };

        let mut needs_scn = false;
        let mut plans: Vec<(SnmpOid, OidKind)> = Vec::with_capacity(to_resolve.len());

        for oid in to_resolve {
            let oid_str = oid.to_string();
            let kind = profile
                .registry()
                .iter()
                .find(|m| m.oid == oid_str)
                .map(|m| m.kind)
                .unwrap_or(OidKind::Exact);
            needs_scn |= kind == OidKind::ScnIndexed;
            plans.push((oid.clone(), kind));
        }

        let scn = if needs_scn {
            match self.fetch_scn().await? {
                Some(ascii) => ascii.to_utc_index(),
                None => {
                    tracing::error!(target: "resolve_oids", profile=?profile.to_string(), "Bug: snmp profile must have scn.");
                    return Err(SnmpError::ScnError {
                        profile: profile.to_string(),
                        message: "Scn not found".to_string(),
                    });
                }
            }
        } else {
            String::new()
        };

        plans
            .into_iter()
            .map(|(oid, kind)| match kind {
                OidKind::Exact => Ok(oid),
                OidKind::Scalar => SnmpOid::parse(&format!("{}.0", oid))
                    .map_err(|e| SnmpError::ResolveOid(e.to_string())),
                OidKind::ScnIndexed => SnmpOid::parse(&format!("{}{}", oid, scn))
                    .map_err(|e| SnmpError::ResolveOid(e.to_string())),
            })
            .collect()
    }

    /// GET: обогащает элементы метаданными профиля и резолвит OID.
    pub async fn resolve_get(
        &self,
        items: Vec<SnmpGetQueryItem>,
    ) -> Result<Vec<ResolvedItem>, SnmpError> {
        let mut resolved = Vec::with_capacity(items.len());

        for item in items {
            let metadata = self.profile.and_then(|p| p.get_metadata_by_oid(&item.oid));
            let parser = item
                .business_value_parser
                .or_else(|| metadata.as_ref().and_then(|m| m.parser));
            let name = item
                .name
                .or_else(|| metadata.as_ref().map(|m| m.name.to_string()));

            resolved.push(ResolvedItem {
                oid: item.oid,
                name,
                parser,
            });
        }

        let oids: Vec<SnmpOid> = resolved.iter().map(|i| i.oid.clone()).collect();
        let resolved_oids = self.resolve_oids(&oids).await?;
        for (item, oid) in resolved.iter_mut().zip(resolved_oids) {
            item.oid = oid;
        }

        Ok(resolved)
    }

    /// SET: только SCN-резолюция OID (значения уже закодированы в `SnmpSetItem`).
    pub async fn resolve_set(
        &self,
        items: Vec<SnmpSetItem>,
    ) -> Result<Vec<SnmpSetItem>, SnmpError> {
        let oids: Vec<SnmpOid> = items.iter().map(|i| i.oid.clone()).collect();
        let resolved = self.resolve_oids(&oids).await?;

        Ok(items
            .into_iter()
            .zip(resolved)
            .map(|(mut item, oid)| {
                item.oid = oid;
                item
            })
            .collect())
    }
}
