use derive_more::Display;

use crate::snmp::{
    builders::OidValueBuilderFn,
    parsers::OidValueParserFn,
    value::SnmpValueType,
};

#[derive(Clone, Copy, Display, Debug)]
pub enum AccessType {
    ReadOnly,
    Write,
    ReadWrite,
}

/// Как достроить базовый OID до полного инстанса.
#[derive(Clone, Copy, Display, PartialEq, Eq, Debug)]
pub enum OidKind {
    /// OID — уже полный инстанс, ничего не дописываем.
    Exact,
    /// Скаляр: к базовому OID дописывается ".0".
    Scalar,
    /// Колонка таблицы, инстанс = SCN контроллера.
    ScnIndexed,
}

#[derive(Clone, Debug)]
pub struct OidMetadata {
    /// Базовый OID (definition). Для `Exact` — уже полный инстанс.
    pub oid: &'static str,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
    pub parser: Option<OidValueParserFn>,
    pub builder: Option<OidValueBuilderFn>,
    pub kind: OidKind,
    pub access: AccessType,
    pub syntax: SnmpValueType,
}
