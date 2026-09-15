mod properties;
pub use properties::*;
mod metadata;
pub use metadata::*;

pub const STAGE_ALIAS: &str = "Stage";
pub const STAGE_ALIASES: &[&str] = &["stage", "phase", "фаза"];
pub const CURRENT_STAGE_STR: &str = "Current stage";
pub const SET_STAGE_STR: &str = "Set stage";
