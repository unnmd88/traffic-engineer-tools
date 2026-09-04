use crate::monitor::application::config::QuerySnmpGet;

//Валидированный запрос (Application превращает YAML в это)
#[derive(Clone, Debug)]
pub enum UseCaseQuery {
    SnmpGet(QuerySnmpGet), // IpAddr, port, Community, SnmpProfile, Vec<SnmpOid>
                           //SnmpSet(QuerySnmpSet),
                           //HttpRead(QueryHttpRead), // url, парсер режима, ...
}
