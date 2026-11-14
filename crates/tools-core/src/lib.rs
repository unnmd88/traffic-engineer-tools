//! Библиотека для задач организации дорожного движения (ОДД).
//!
//! ## Готовые фичи
//! - [`scn`] — конвертация SCN (ASCII ↔ dotted ↔ scn-index ↔ utc-index);
//! - [`monitor`] — опрос и управление ДК по сценариям.

#![warn(missing_docs)]

mod constants;
mod utils;

pub mod polling;

pub mod monitor;

pub mod error;
pub mod potok_log;
pub mod scn;
pub mod stage;

pub mod snmp;
pub use constants::{DT_FMT, DT_FMT_WITH_MICROSECONDS};
pub use error::Error;
pub use polling::PollErrorContext;
pub use scn::{Scn, ScnError};
pub use snmp::SnmpError;
