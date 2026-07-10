use serde_json;

pub trait Presentable {
    fn to_pretty_string(&self) -> String;
    fn to_json(&self) -> serde_json::Value;
}
