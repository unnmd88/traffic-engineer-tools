mod error;
mod outcome;
mod snmp;

use thiserror::Error;
use tokio::time::{Duration, Instant};

use crate::polling::AttemptConfig;

pub use error::{ProbeBuildError, ProbeError, SnmpQueryError};
pub use outcome::{OutcomeStatus, ProbeExecution, ProbeOutcome};
pub use snmp::{
    RawSnmpOidItem, RawSnmpSetItem, SnmpGetProbe, SnmpGetSpec, SnmpSetProbe, SnmpSetSpec,
    build_timeout,
};

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProbeSpecError {
    #[error("poll interval must be greater than zero")]
    IntervalMustBePositive,
    #[error(
        "poll interval ({interval_ms} ms) must be >= total attempt budget \
         ({budget_ms} ms = timeout * (retries + 1) + retry_delay * retries)"
    )]
    IntervalShorterThanAttemptBudget { interval_ms: u64, budget_ms: u64 },
}

#[derive(Clone, Debug)]
pub enum ProbeSpec {
    SnmpGet {
        request: SnmpGetSpec,
        interval: Duration,
        limit: u64,
        attempt: AttemptConfig,
    },
    SnmpSet {
        request: SnmpSetSpec,
        interval: Duration,
        limit: u64,
        attempt: AttemptConfig,
    },
}

impl ProbeSpec {
    pub fn snmp_get(
        request: SnmpGetSpec,
        interval: Duration,
        limit: u64,
        attempt: AttemptConfig,
    ) -> Result<Self, ProbeSpecError> {
        Self::validate(interval, attempt)?;
        Ok(Self::SnmpGet {
            request,
            interval,
            limit,
            attempt,
        })
    }

    pub fn snmp_set(
        request: SnmpSetSpec,
        interval: Duration,
        limit: u64,
        attempt: AttemptConfig,
    ) -> Result<Self, ProbeSpecError> {
        Self::validate(interval, attempt)?;
        Ok(Self::SnmpSet {
            request,
            interval,
            limit,
            attempt,
        })
    }

    fn validate(interval: Duration, attempt: AttemptConfig) -> Result<(), ProbeSpecError> {
        if interval.is_zero() {
            return Err(ProbeSpecError::IntervalMustBePositive);
        }
        let budget = attempt.budget();
        if interval < budget {
            return Err(ProbeSpecError::IntervalShorterThanAttemptBudget {
                interval_ms: interval.as_millis() as u64,
                budget_ms: budget.as_millis() as u64,
            });
        }
        Ok(())
    }

    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet { request, .. } => format!("{}:{}", request.host, request.port),
            Self::SnmpSet { request, .. } => format!("{}:{}", request.host, request.port),
        }
    }

    pub fn interval(&self) -> Duration {
        match self {
            Self::SnmpGet { interval, .. } => *interval,
            Self::SnmpSet { interval, .. } => *interval,
        }
    }

    pub fn limit(&self) -> u64 {
        match self {
            Self::SnmpGet { limit, .. } => *limit,
            Self::SnmpSet { limit, .. } => *limit,
        }
    }

    pub fn attempt(&self) -> AttemptConfig {
        match self {
            Self::SnmpGet { attempt, .. } => *attempt,
            Self::SnmpSet { attempt, .. } => *attempt,
        }
    }

    pub fn build_timeout(&self) -> Duration {
        match self {
            Self::SnmpGet { attempt, .. } | Self::SnmpSet { attempt, .. } => build_timeout(attempt),
        }
    }

    pub async fn build(self) -> Result<Probe, ProbeBuildError> {
        match self {
            Self::SnmpGet {
                request,
                interval,
                limit: _,
                attempt,
            } => request.build(attempt, interval).await.map(Probe::SnmpGet),
            Self::SnmpSet {
                request,
                interval,
                limit: _,
                attempt,
            } => request.build(attempt, interval).await.map(Probe::SnmpSet),
        }
    }
}

pub enum Probe {
    SnmpGet(SnmpGetProbe),
    SnmpSet(SnmpSetProbe),
}

impl Probe {
    pub fn next_deadline(&self) -> Instant {
        match self {
            Self::SnmpGet(p) => p.next_deadline(),
            Self::SnmpSet(p) => p.next_deadline(),
        }
    }

    pub async fn do_work(&mut self) -> Result<ProbeOutcome, ProbeError> {
        match self {
            Self::SnmpGet(p) => p.do_work().await,
            Self::SnmpSet(p) => p.do_work().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SnmpGetSpec {
        SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
            .unwrap()
    }

    fn attempt(timeout_ms: u64, retries: u8, retry_delay_ms: u64) -> AttemptConfig {
        AttemptConfig::try_new(
            Duration::from_millis(timeout_ms),
            retries,
            Duration::from_millis(retry_delay_ms),
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_interval() {
        let err = ProbeSpec::snmp_get(request(), Duration::from_millis(0), 0, attempt(100, 0, 0))
            .unwrap_err();
        assert!(matches!(err, ProbeSpecError::IntervalMustBePositive));
    }

    #[test]
    fn rejects_interval_shorter_than_budget() {
        // budget = 100*(2+1) + 50*2 = 400ms
        let err =
            ProbeSpec::snmp_get(request(), Duration::from_millis(300), 0, attempt(100, 2, 50))
                .unwrap_err();
        assert!(matches!(err, ProbeSpecError::IntervalShorterThanAttemptBudget { .. }));
    }

    #[test]
    fn accepts_interval_equal_to_budget() {
        let spec =
            ProbeSpec::snmp_get(request(), Duration::from_millis(400), 0, attempt(100, 2, 50))
                .unwrap();
        assert_eq!(spec.interval(), Duration::from_millis(400));
    }
}
