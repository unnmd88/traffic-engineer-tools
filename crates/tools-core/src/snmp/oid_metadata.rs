use derive_more::Display;

use crate::snmp::{builders::OidValueBuilderFn, parsers::OidValueParserFn, value::SnmpValueType};

#[derive(Clone, Copy, Display, Debug)]
pub enum AccessType {
    ReadOnly,
    Write,
    ReadWrite,
}

#[derive(Clone, Copy, Display, PartialEq, Eq, Debug)]
pub enum Requirenment {
    Scn,
}

#[derive(Clone, Debug)]
pub struct OidMetadata {
    pub oid: &'static str,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
    pub parser: Option<OidValueParserFn>,
    pub builder: Option<OidValueBuilderFn>,
    pub requires: Option<&'static [Requirenment]>,
    pub access: AccessType,
    pub syntax: SnmpValueType,
}
