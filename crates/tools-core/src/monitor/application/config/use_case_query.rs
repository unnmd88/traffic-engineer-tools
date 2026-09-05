use crate::monitor::{
    application::config::QuerySnmpGet,
    task::{Protocol, TypeQuery},
};

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
