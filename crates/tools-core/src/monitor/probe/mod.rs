mod error;
mod snmp;

use async_trait::async_trait;

use crate::{
    polling::{AttemptConfig, AttemptError, Pollable},
    snmp::SnmpGetSample,
};

pub use error::{ProbeBuildError, SnmpQueryError};
pub use snmp::{
    RawSnmpOidItem, RawSnmpSetItem, SnmpGetProbe, SnmpGetSpec, SnmpSetProbe, SnmpSetSpec,
    build_timeout,
};

#[derive(Clone, Debug)]
pub enum ProbeSpec {
    SnmpGet(SnmpGetSpec),
    SnmpSet(SnmpSetSpec),
}

impl ProbeSpec {
    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(s) => format!("{}:{}", s.host, s.port),
            Self::SnmpSet(s) => format!("{}:{}", s.host, s.port),
        }
    }

    pub async fn build(self, attempt: AttemptConfig) -> Result<BuiltProbe, ProbeBuildError> {
        match self {
            Self::SnmpGet(s) => s.build(attempt).await.map(BuiltProbe::SnmpGet),
            Self::SnmpSet(s) => s.build(attempt).await.map(BuiltProbe::SnmpSet),
        }
    }
}

pub enum BuiltProbe {
    SnmpGet(SnmpGetProbe),
    SnmpSet(SnmpSetProbe),
}

#[derive(Clone, Debug)]
pub enum ProbeOutput {
    SnmpGet(Vec<SnmpGetSample>),
    SnmpSet(()),
}

#[async_trait]
impl Pollable for BuiltProbe {
    type Output = ProbeOutput;

    async fn poll(&self) -> Result<ProbeOutput, AttemptError> {
        match self {
            Self::SnmpGet(p) => p.poll().await.map(ProbeOutput::SnmpGet),
            Self::SnmpSet(p) => p.poll().await.map(ProbeOutput::SnmpSet),
        }
    }
}
