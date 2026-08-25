use crate::snmp::parsers::OidValueParserFn;

pub struct OidMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub alias: &'static str,
    pub parser: Option<OidValueParserFn>,
}
