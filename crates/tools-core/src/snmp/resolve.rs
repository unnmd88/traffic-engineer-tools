//! Подготовка запроса и резолюция в финальный план опроса.
//!
//! [`Resolver`] applies profile metadata and constructs final OID instances.
//! SCN discovery is performed separately so callers can observe and report it.
//!
//! Контракт:
//! - без профиля — библиотека ничего не резолвит (raw-режим);
//! - с профилем — библиотека отвечает за резолюцию: если OID требует SCN,
//!   а получить его не удалось, резолюция завершается ошибкой (никогда не
//!   опрашиваем «сырой» OID, когда нужен SCN).

use crate::{
    scn::Scn,
    snmp::{
        ParseError, SnmpError,
        builders::encode_by_type,
        oid::SnmpOid,
        oid_metadata::OidKind,
        parsers::OidValueDisplayFn,
        profiles::SnmpProfile,
        value::{SnmpValue, SnmpValueType},
    },
};

#[derive(Debug, Clone)]
pub struct SnmpGetItem {
    pub oid: SnmpOid,
    pub name: Option<String>,
    pub parser: Option<OidValueDisplayFn>,
}

#[derive(Debug, Clone)]
pub struct SnmpSetItem {
    pub name: Option<String>,
    pub oid: SnmpOid,
    pub value: SnmpValue,
}

/// Кодирует сырое значение SET в `SnmpValue`:
/// 1) явный `value_type` → 2) builder из реестра профиля → 3) ошибка.
pub fn encode_set_value(
    oid: &SnmpOid,
    value: &str,
    value_type: Option<&str>,
    profile: Option<&SnmpProfile>,
) -> Result<SnmpValue, ParseError> {
    if let Some(ty) = value_type {
        let ty: SnmpValueType = ty.parse().map_err(|e| ParseError::Common { message: e })?;
        return encode_by_type(ty, value);
    }

    if let Some(builder) = profile
        .and_then(|p| p.get_metadata_by_oid(oid))
        .and_then(|m| m.builder)
    {
        return builder(value);
    }

    Err(ParseError::Common {
        message: format!("no builder and no value_type for oid {oid}"),
    })
}

pub struct Resolver {
    profile: Option<SnmpProfile>,
}

impl Resolver {
    pub fn new(profile: Option<SnmpProfile>) -> Self {
        Self { profile }
    }

    /// Returns whether any selected OID requires a device-specific SCN index.
    pub fn needs_scn(&self, oids: &[SnmpOid]) -> bool {
        self.profile.is_some_and(|profile| {
            oids.iter().any(|oid| {
                profile.registry().iter().any(|metadata| {
                    metadata.oid == oid.to_string() && metadata.kind == OidKind::ScnIndexed
                })
            })
        })
    }

    /// Достраивает OID до полного инстанса по `OidKind` из реестра профиля:
    /// скаляры получают ".0", SCN-колонки — индекс контроллера.
    pub fn resolve_oids(
        &self,
        to_resolve: &[SnmpOid],
        scn: Option<&Scn>,
    ) -> Result<Vec<SnmpOid>, SnmpError> {
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

        let scn_suffix = if needs_scn {
            let scn = scn.ok_or_else(|| SnmpError::ScnError {
                profile: profile.to_string(),
                message: "SCN is required before resolving these OIDs".to_string(),
            })?;
            Some(
                SnmpOid::parse(&scn.to_utc_index())
                    .map_err(|error| SnmpError::ResolveOid(error.to_string()))?,
            )
        } else {
            None
        };

        plans
            .into_iter()
            .map(|(oid, kind)| match kind {
                OidKind::Exact => Ok(oid),
                OidKind::Scalar => Ok(oid.child(0)),
                OidKind::ScnIndexed => match scn_suffix.as_ref() {
                    Some(suffix) => Ok(oid.with_suffix(suffix)),
                    None => Err(SnmpError::ScnError {
                        profile: profile.to_string(),
                        message: "Scn not found".to_string(),
                    }),
                },
            })
            .collect()
    }

    /// GET: обогащает элементы метаданными профиля и резолвит OID.
    pub fn resolve_get(
        &self,
        items: Vec<SnmpGetItem>,
        scn: Option<&Scn>,
    ) -> Result<Vec<SnmpGetItem>, SnmpError> {
        let mut resolved = Vec::with_capacity(items.len());

        for mut item in items {
            let metadata = self.profile.and_then(|p| p.get_metadata_by_oid(&item.oid));
            item.name = item
                .name
                .or_else(|| metadata.as_ref().map(|m| m.name.to_string()));
            item.parser = item
                .parser
                .or_else(|| metadata.as_ref().and_then(|m| m.display));
            resolved.push(item);
        }

        let oids: Vec<SnmpOid> = resolved.iter().map(|i| i.oid.clone()).collect();
        let resolved_oids = self.resolve_oids(&oids, scn)?;
        for (item, oid) in resolved.iter_mut().zip(resolved_oids) {
            item.oid = oid;
        }

        Ok(resolved)
    }

    /// SET: только SCN-резолюция OID (значения уже закодированы в `SnmpSetItem`).
    pub fn resolve_set(
        &self,
        items: Vec<SnmpSetItem>,
        scn: Option<&Scn>,
    ) -> Result<Vec<SnmpSetItem>, SnmpError> {
        let oids: Vec<SnmpOid> = items.iter().map(|i| i.oid.clone()).collect();
        let resolved = self.resolve_oids(&oids, scn)?;

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
