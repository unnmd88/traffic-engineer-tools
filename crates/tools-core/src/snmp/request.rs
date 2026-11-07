use std::net::{IpAddr, SocketAddr};

use serde::Deserialize;

use super::{
    ParseError, SnmpQueryError,
    community::Community,
    oid::SnmpOid,
    profiles::SnmpProfile,
    resolve::{SnmpGetItem, SnmpSetItem, encode_set_value},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSnmpSetItem {
    pub name: Option<String>,
    pub oid: String,
    pub value: String,
    #[serde(alias = "syntax")]
    pub value_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnmpGetSpec {
    profile: Option<SnmpProfile>,
    host: IpAddr,
    port: u16,
    community: Community,
    oids: Vec<SnmpGetItem>,
}

#[derive(Debug, Clone)]
pub struct SnmpSetSpec {
    profile: Option<SnmpProfile>,
    host: IpAddr,
    port: u16,
    community: Community,
    community_r: Option<Community>,
    sets: Vec<SnmpSetItem>,
}

impl SnmpGetSpec {
    pub const fn new(
        target: SocketAddr,
        community: Community,
        profile: Option<SnmpProfile>,
        oids: Vec<SnmpGetItem>,
    ) -> Self {
        Self {
            profile,
            host: target.ip(),
            port: target.port(),
            community,
            oids,
        }
    }

    pub fn from_raw(
        host: String,
        port: u16,
        community: String,
        profile: Option<String>,
        oids: Vec<RawSnmpOidItem>,
    ) -> Result<Self, SnmpQueryError> {
        let host = parse_ip(&host)?;
        let community = parse_community(community)?;
        let profile = parse_profile(profile)?;

        let oids = oids
            .into_iter()
            .enumerate()
            .map(|(pos, raw)| {
                let oid = lookup_oid(&raw.oid, profile.as_ref(), pos)?;
                Ok(SnmpGetItem {
                    name: raw.name,
                    oid,
                    parser: None,
                })
            })
            .collect::<Result<Vec<_>, SnmpQueryError>>()?;

        Ok(Self::new(SocketAddr::new(host, port), community, profile, oids))
    }

    pub const fn host(&self) -> IpAddr {
        self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub const fn community(&self) -> &Community {
        &self.community
    }

    pub const fn profile(&self) -> Option<SnmpProfile> {
        self.profile
    }

    pub const fn oids(&self) -> &[SnmpGetItem] {
        self.oids.as_slice()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (IpAddr, u16, Community, Option<SnmpProfile>, Vec<SnmpGetItem>) {
        (self.host, self.port, self.community, self.profile, self.oids)
    }
}

impl SnmpSetSpec {
    pub fn try_new(
        target: SocketAddr,
        community_r: Option<Community>,
        community_w: Community,
        profile: Option<SnmpProfile>,
        sets: Vec<SnmpSetItem>,
    ) -> Result<Self, SnmpQueryError> {
        for (pos, item) in sets.iter().enumerate() {
            async_snmp::Value::try_from(&item.value).map_err(|source| {
                SnmpQueryError::InvalidSetValue {
                    pos,
                    source: Box::new(source),
                }
            })?;
        }
        Ok(Self {
            profile,
            host: target.ip(),
            port: target.port(),
            community: community_w,
            community_r,
            sets,
        })
    }

    pub fn from_raw(
        host: String,
        port: u16,
        community_r: Option<String>,
        community_w: String,
        profile: Option<String>,
        sets: Vec<RawSnmpSetItem>,
    ) -> Result<Self, SnmpQueryError> {
        let host = parse_ip(&host)?;
        let community = parse_community(community_w)?;
        let community_r = community_r.map(parse_community).transpose()?;
        let profile = parse_profile(profile)?;

        let sets = sets
            .into_iter()
            .enumerate()
            .map(|(pos, raw)| {
                let oid = lookup_oid(&raw.oid, profile.as_ref(), pos)?;
                let value =
                    encode_set_value(&oid, &raw.value, raw.value_type.as_deref(), profile.as_ref())
                        .map_err(|e| SnmpQueryError::Other(e.to_string()))?;
                Ok(SnmpSetItem {
                    name: raw.name,
                    oid,
                    value,
                })
            })
            .collect::<Result<Vec<_>, SnmpQueryError>>()?;

        Self::try_new(SocketAddr::new(host, port), community_r, community, profile, sets)
    }

    pub const fn host(&self) -> IpAddr {
        self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub const fn community(&self) -> &Community {
        &self.community
    }

    pub const fn community_r(&self) -> Option<&Community> {
        self.community_r.as_ref()
    }

    pub const fn profile(&self) -> Option<SnmpProfile> {
        self.profile
    }

    pub const fn sets(&self) -> &[SnmpSetItem] {
        self.sets.as_slice()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (IpAddr, u16, Option<Community>, Community, Option<SnmpProfile>, Vec<SnmpSetItem>) {
        (self.host, self.port, self.community_r, self.community, self.profile, self.sets)
    }
}

fn parse_ip(ip: &str) -> Result<IpAddr, SnmpQueryError> {
    ip.parse::<IpAddr>()
        .map_err(|_| SnmpQueryError::InvalidIpAddress { ip: ip.to_string() })
}

fn parse_community(community: String) -> Result<Community, SnmpQueryError> {
    Community::try_from(community).map_err(|e| match e {
        ParseError::CantBeEmpty { .. } => SnmpQueryError::SnmpCommunityIsEmpty,
        ParseError::InvalidLength {
            min, max, provide, ..
        } => SnmpQueryError::SnmpCommunityInvalidLength { min, max, provide },
        ParseError::Common { message } => SnmpQueryError::Other(message),
        _ => SnmpQueryError::Other("Can't parse community string".to_string()),
    })
}

fn parse_profile(profile: Option<String>) -> Result<Option<SnmpProfile>, SnmpQueryError> {
    profile
        .map(|p| p.parse::<SnmpProfile>())
        .transpose()
        .map_err(|e| SnmpQueryError::InvalidSnmpProfile { message: e })
}

fn lookup_oid(
    raw: &str,
    profile: Option<&SnmpProfile>,
    pos: usize,
) -> Result<SnmpOid, SnmpQueryError> {
    match profile {
        Some(p) => p.resolve_oid(raw).map_err(|e| match e {
            ParseError::UnknownAlias { alias } => SnmpQueryError::UnknownAlias { pos, alias },
            e => SnmpQueryError::Other(e.to_string()),
        }),
        None => SnmpOid::parse(raw.trim()).map_err(|_| SnmpQueryError::SnmpProfileMustBeProvided {
            message: "SNMP profile is required for auto search oid by name".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snmp::{SnmpError, business_value::BusinessValue, value::SnmpValue};

    fn raw(oid: &str) -> RawSnmpOidItem {
        RawSnmpOidItem {
            name: None,
            oid: oid.to_string(),
        }
    }

    #[test]
    fn resolves_numeric_oids_without_profile() {
        let q = SnmpGetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            "public".to_string(),
            None,
            vec![raw("1.3.6.1.4.1.1618.3.7.2.11.2")],
        )
        .unwrap();

        assert_eq!(q.host(), "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(q.oids().len(), 1);
        assert!(q.profile().is_none());
    }

    #[test]
    fn rejects_invalid_ip() {
        let err =
            SnmpGetSpec::from_raw("not-an-ip".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap_err();

        assert!(matches!(err, SnmpQueryError::InvalidIpAddress { .. }));
    }

    #[test]
    fn rejects_empty_community() {
        let err = SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, String::new(), None, vec![])
            .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpCommunityIsEmpty));
    }

    #[test]
    fn rejects_alias_without_profile() {
        let err = SnmpGetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            "public".to_string(),
            None,
            vec![raw("some_alias")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpProfileMustBeProvided { .. }));
    }

    fn raw_set(oid: &str, value: &str) -> RawSnmpSetItem {
        RawSnmpSetItem {
            name: None,
            oid: oid.to_string(),
            value: value.to_string(),
            value_type: None,
        }
    }

    #[test]
    fn snmp_set_numeric_oid_with_explicit_type() {
        let q = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![RawSnmpSetItem {
                name: None,
                oid: "1.3.6.1.4.1.999.1.0".to_string(),
                value: "0x01".to_string(),
                value_type: Some("octet_string".to_string()),
            }],
        )
        .unwrap();

        assert_eq!(q.sets().len(), 1);
        assert!(matches!(
            q.sets()[0].value,
            SnmpValue::OctetString(ref b) if b == &vec![0x01]
        ));
    }

    #[test]
    fn snmp_set_alias_with_profile_uses_builder() {
        let q = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            Some("swarco".to_string()),
            vec![raw_set("set_stage", "3")],
        )
        .unwrap();

        assert_eq!(q.sets().len(), 1);
        assert!(matches!(q.sets()[0].value, SnmpValue::Gauge32(3)));
    }

    #[test]
    fn snmp_set_rejects_alias_without_profile() {
        let err = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![raw_set("stage", "3")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::SnmpProfileMustBeProvided { .. }));
    }

    #[test]
    fn snmp_set_rejects_without_builder_or_type() {
        let err = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "public".to_string(),
            None,
            vec![raw_set("1.3.6.1.4.1.999.1.0", "3")],
        )
        .unwrap_err();

        assert!(matches!(err, SnmpQueryError::Other(_)));
    }

    #[test]
    fn typed_get_preserves_custom_oids_names_and_parsers_without_a_profile() {
        let oid = SnmpOid::parse("1.3.6.1.4.1.999.123.0").unwrap();
        let request = SnmpGetSpec::new(
            "127.0.0.1:1161".parse().unwrap(),
            "public".parse().unwrap(),
            None,
            vec![SnmpGetItem {
                oid: oid.clone(),
                name: Some("custom".to_string()),
                parser: Some(|_| Ok(BusinessValue::Text("custom parser".to_string()))),
            }],
        );

        assert_eq!(request.port(), 1161);
        assert!(request.profile().is_none());
        assert_eq!(request.oids()[0].oid, oid);
        assert_eq!(request.oids()[0].name.as_deref(), Some("custom"));
        let parsed = request.oids()[0].parser.unwrap()(&SnmpValue::Integer(7)).unwrap();
        assert!(matches!(parsed, BusinessValue::Text(value) if value == "custom parser"));
    }

    #[test]
    fn raw_and_typed_gets_produce_equivalent_configurations() {
        let raw = SnmpGetSpec::from_raw(
            "127.0.0.1".to_string(),
            1161,
            "public".to_string(),
            Some("potok_ug405".to_string()),
            vec![RawSnmpOidItem {
                name: Some("stage".to_string()),
                oid: "stage".to_string(),
            }],
        )
        .unwrap();
        let typed = SnmpGetSpec::new(
            "127.0.0.1:1161".parse().unwrap(),
            "public".parse().unwrap(),
            Some(SnmpProfile::PotokUg405),
            vec![SnmpGetItem {
                oid: SnmpProfile::PotokUg405.resolve_oid("stage").unwrap(),
                name: Some("stage".to_string()),
                parser: None,
            }],
        );

        assert_eq!(raw.host(), typed.host());
        assert_eq!(raw.port(), typed.port());
        assert_eq!(raw.community(), typed.community());
        assert_eq!(raw.profile(), typed.profile());
        assert_eq!(raw.oids()[0].oid, typed.oids()[0].oid);
        assert_eq!(raw.oids()[0].name, typed.oids()[0].name);
    }

    #[test]
    fn raw_and_typed_sets_preserve_encoded_values_and_both_communities() {
        let raw = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            1161,
            Some("public".to_string()),
            "private".to_string(),
            Some("potok_ug405".to_string()),
            vec![raw_set("set_stage", "3")],
        )
        .unwrap();
        let typed = SnmpSetSpec::try_new(
            "127.0.0.1:1161".parse().unwrap(),
            Some("public".parse().unwrap()),
            "private".parse().unwrap(),
            Some(SnmpProfile::PotokUg405),
            vec![SnmpSetItem {
                oid: SnmpProfile::PotokUg405.resolve_oid("set_stage").unwrap(),
                name: None,
                value: SnmpValue::OctetString(vec![4]),
            }],
        )
        .unwrap();

        assert_eq!(raw.host(), typed.host());
        assert_eq!(raw.port(), typed.port());
        assert_eq!(raw.community(), typed.community());
        assert_eq!(raw.community_r(), typed.community_r());
        assert_eq!(raw.profile(), typed.profile());
        assert_eq!(raw.sets()[0].oid, typed.sets()[0].oid);
        assert_eq!(
            async_snmp::Value::try_from(&raw.sets()[0].value).unwrap(),
            async_snmp::Value::try_from(&typed.sets()[0].value).unwrap(),
        );
    }

    #[test]
    fn typed_set_accepts_all_values_supported_by_the_encoder() {
        for value in [
            SnmpValue::Integer(-1),
            SnmpValue::Gauge32(3),
            SnmpValue::Counter32(4),
            SnmpValue::Counter64(5),
            SnmpValue::OctetString(vec![0, 255]),
            SnmpValue::Oid(SnmpOid::parse("1.3.6.1.2.1.1.1.0").unwrap()),
            SnmpValue::IpAddress([127, 0, 0, 1]),
            SnmpValue::Null,
        ] {
            let request = SnmpSetSpec::try_new(
                "127.0.0.1:161".parse().unwrap(),
                None,
                "private".parse().unwrap(),
                None,
                vec![SnmpSetItem {
                    oid: SnmpOid::parse("1.3.6.1.4.1.999.1.0").unwrap(),
                    name: None,
                    value,
                }],
            )
            .unwrap();
            assert!(async_snmp::Value::try_from(&request.sets()[0].value).is_ok());
        }
    }

    #[test]
    fn typed_set_rejects_unencodable_values_with_their_position_and_source() {
        for value in [
            SnmpValue::TimeTicks(6),
            SnmpValue::Opaque(vec![1]),
            SnmpValue::NoSuchObject,
            SnmpValue::NoSuchInstance,
            SnmpValue::EndOfMibView,
            SnmpValue::Other("unsupported".to_string()),
            SnmpValue::Unknown {
                tag: 255,
                data: vec![1],
            },
        ] {
            let oid = SnmpOid::parse("1.3.6.1.4.1.999.1.0").unwrap();
            let error = SnmpSetSpec::try_new(
                "127.0.0.1:161".parse().unwrap(),
                None,
                "private".parse().unwrap(),
                None,
                vec![
                    SnmpSetItem {
                        oid: oid.clone(),
                        name: None,
                        value: SnmpValue::Integer(1),
                    },
                    SnmpSetItem {
                        oid,
                        name: None,
                        value,
                    },
                ],
            )
            .unwrap_err();
            assert!(matches!(
                error,
                SnmpQueryError::InvalidSetValue {
                    pos: 1,
                    source,
                } if matches!(source.as_ref(), SnmpError::UnsupportedForSet { .. })
            ));
        }
    }

    #[test]
    fn raw_set_checks_read_and_write_communities() {
        for (read, write) in [
            (Some(String::new()), "private".to_string()),
            (None, String::new()),
        ] {
            let error =
                SnmpSetSpec::from_raw("127.0.0.1".to_string(), 161, read, write, None, vec![])
                    .unwrap_err();
            assert!(matches!(error, SnmpQueryError::SnmpCommunityIsEmpty));
        }
        for (read, write) in [
            (Some("x".repeat(33)), "private".to_string()),
            (None, "x".repeat(33)),
        ] {
            let error =
                SnmpSetSpec::from_raw("127.0.0.1".to_string(), 161, read, write, None, vec![])
                    .unwrap_err();
            assert!(matches!(
                error,
                SnmpQueryError::SnmpCommunityInvalidLength { provide: 33, .. }
            ));
        }
    }

    #[test]
    fn raw_set_uses_the_same_encoder_validation_as_typed_set() {
        let error = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "private".to_string(),
            None,
            vec![RawSnmpSetItem {
                name: None,
                oid: "1.3.6.1.4.1.999.1.0".to_string(),
                value: "6".to_string(),
                value_type: Some("time_ticks".to_string()),
            }],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SnmpQueryError::InvalidSetValue {
                pos: 0,
                source,
            } if matches!(source.as_ref(), SnmpError::UnsupportedForSet { .. })
        ));
    }

    #[test]
    fn typed_specs_keep_empty_requests_and_custom_ports_available() {
        for port in [0, 1161] {
            let get = SnmpGetSpec::new(
                SocketAddr::new("::1".parse().unwrap(), port),
                "public".parse().unwrap(),
                None,
                vec![],
            );
            let set = SnmpSetSpec::try_new(
                SocketAddr::new("::1".parse().unwrap(), port),
                None,
                "private".parse().unwrap(),
                None,
                vec![],
            )
            .unwrap();
            assert_eq!(get.host(), "::1".parse::<IpAddr>().unwrap());
            assert_eq!(set.host(), get.host());
            assert_eq!(get.port(), port);
            assert_eq!(set.port(), port);
            assert!(get.oids().is_empty());
            assert!(set.sets().is_empty());
        }
    }
}
