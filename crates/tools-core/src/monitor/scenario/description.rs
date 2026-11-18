use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::{
    polling::AttemptConfig,
    snmp::{SnmpOid, SnmpProfile, SnmpValue},
};

use super::{PollSchedule, ScenarioSpec};

/// A configured GET item, before scenario preparation resolves its final instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SnmpGetItemDescription {
    /// The optional consumer-supplied name.
    pub name: Option<String>,
    /// The validated numeric OID definition from the configuration.
    pub oid: SnmpOid,
}

/// A configured SET item, before scenario preparation resolves its final instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SnmpSetItemDescription {
    /// The optional consumer-supplied name.
    pub name: Option<String>,
    /// The validated numeric OID definition from the configuration.
    pub oid: SnmpOid,
    /// The encoded value the operation will send.
    pub value: SnmpValue,
}

/// A serializable scenario description that contains no transport credentials.
///
/// Descriptions preserve validated request parameters, not the original input spelling
/// or parser callbacks. They are passive observation records, not executable configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ScenarioDescription {
    /// A repeating SNMP GET operation.
    #[non_exhaustive]
    SnmpGet {
        /// The configured remote address.
        target: SocketAddr,
        /// The selected device profile, if any.
        profile: Option<SnmpProfile>,
        /// Requested OID definitions in configuration order.
        oids: Vec<SnmpGetItemDescription>,
        /// The scenario's operation schedule.
        schedule: PollSchedule,
        /// The timeout and retries for each complete operation.
        attempt: AttemptConfig,
    },
    /// A repeating SNMP SET operation.
    #[non_exhaustive]
    SnmpSet {
        /// The configured remote address.
        target: SocketAddr,
        /// The selected device profile, if any.
        profile: Option<SnmpProfile>,
        /// Requested OID definitions and encoded values in configuration order.
        sets: Vec<SnmpSetItemDescription>,
        /// The scenario's operation schedule.
        schedule: PollSchedule,
        /// The timeout and retries for each complete operation.
        attempt: AttemptConfig,
    },
}

impl From<&ScenarioSpec> for ScenarioDescription {
    fn from(spec: &ScenarioSpec) -> Self {
        match spec {
            ScenarioSpec::SnmpGet(spec) => {
                let request = spec.request();
                Self::SnmpGet {
                    target: SocketAddr::new(request.host(), request.port()),
                    profile: request.profile(),
                    oids: request
                        .oids()
                        .iter()
                        .map(|item| SnmpGetItemDescription {
                            name: item.name.clone(),
                            oid: item.oid.clone(),
                        })
                        .collect(),
                    schedule: spec.schedule(),
                    attempt: spec.attempt(),
                }
            }
            ScenarioSpec::SnmpSet(spec) => {
                let request = spec.request();
                Self::SnmpSet {
                    target: SocketAddr::new(request.host(), request.port()),
                    profile: request.profile(),
                    sets: request
                        .sets()
                        .iter()
                        .map(|item| SnmpSetItemDescription {
                            name: item.name.clone(),
                            oid: item.oid.clone(),
                            value: item.value.clone(),
                        })
                        .collect(),
                    schedule: spec.schedule(),
                    attempt: spec.attempt(),
                }
            }
        }
    }
}
