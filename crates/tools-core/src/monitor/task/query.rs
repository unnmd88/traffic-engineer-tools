use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SnmpOidItem {
    #[serde(default)]
    pub name: Option<String>,
    pub oid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuerySnmpGet {
    #[serde(default)]
    pub profile: Option<String>,
    pub host: String,
    pub port: u16,
    pub community: String,
    pub oids: Vec<SnmpOidItem>,
}

/// Запрос задачи (десериализуется из YAML; валидация host/community/OID —
/// на этапе `UseCase::build`).
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "query_type", rename_all = "lowercase")]
pub enum UseCaseQuery {
    SnmpGet(QuerySnmpGet),
    // SnmpSet(QuerySnmpSet),
    // HttpRead(QueryHttpRead),
}

impl UseCaseQuery {
    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(q) => format!("{}:{}", q.host, q.port),
        }
    }
}
