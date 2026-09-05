use crate::monitor::task::{Protocol, TypeQuery};

#[derive(Debug, Clone)]
pub struct SnmpOidItem {
    pub name: Option<String>,
    pub oid: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QuerySnmpGet {
    pub profile: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItem>,
}

// Валидированный запрос (Application превращает YAML в это)
#[derive(Clone, Debug)]
pub enum UseCaseQuery {
    SnmpGet(QuerySnmpGet), // IpAddr, port, Community, SnmpProfile, Vec<SnmpOid>
                           // SnmpSet(QuerySnmpSet),
                           // HttpRead(QueryHttpRead), // url, парсер режима, ...
}

impl UseCaseQuery {
    pub fn protocol(&self) -> Protocol {
        match self {
            Self::SnmpGet(_) => Protocol::Snmp,
        }
    }

    pub fn type_query(&self) -> TypeQuery {
        match self {
            Self::SnmpGet(_) => TypeQuery::SnmpGet,
        }
    }

    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(q) => format!("{}:{}", q.host, q.port),
        }
    }
}
