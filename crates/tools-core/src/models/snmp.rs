use std::net::IpAddr;

use serde_json::json;

use crate::{
    presentation::Presentable,
    values::{ControllerValue, Name, SnmpRawValue},
};

#[derive(Debug, Clone)]
pub struct Sample {
    pub oid: String,
    pub name: Option<Name>,
    pub raw: SnmpRawValue,
    pub value: ControllerValue,
}

#[derive(Debug, Clone)]
pub struct SnmpResponse {
    pub target: IpAddr,
    pub timestamp: String,
    pub payload: Vec<Sample>,
}

impl Presentable for SnmpResponse {
    fn to_pretty_string(&self) -> String {
        let mut oids = String::new();
        for oid in &self.payload {
            oids.push_str(&format!(
                "oid={:?} name={:?} raw={:?}, value={:?}",
                oid.oid, oid.name, oid.raw, oid.value
            ));
        }
        format!(
            "target={}\nimestamp={}\n{oids}",
            self.target,
            self.timestamp.clone()
        )
    }

    fn to_json(&self) -> serde_json::Value {
        json!({})
    }
}
