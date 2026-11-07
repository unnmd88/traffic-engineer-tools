use crate::{
    monitor::probe::{SnmpGetSpec, SnmpSetSpec},
    polling::AttemptConfig,
};

use super::{PollSchedule, ScenarioSpecError};

#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Keep small scenario configurations inline"
)]
pub enum ScenarioSpec {
    SnmpGet(SnmpGetScenarioSpec),
    SnmpSet(SnmpSetScenarioSpec),
}

impl ScenarioSpec {
    pub fn target(&self) -> String {
        match self {
            Self::SnmpGet(spec) => format!("{}:{}", spec.request.host, spec.request.port),
            Self::SnmpSet(spec) => format!("{}:{}", spec.request.host, spec.request.port),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnmpGetScenarioSpec {
    pub(super) request: SnmpGetSpec,
    pub(super) schedule: PollSchedule,
    pub(super) attempt: AttemptConfig,
}

impl SnmpGetScenarioSpec {
    pub fn try_new(
        request: SnmpGetSpec,
        schedule: PollSchedule,
        attempt: AttemptConfig,
    ) -> Result<Self, ScenarioSpecError> {
        schedule.validate()?;
        Ok(Self {
            request,
            schedule,
            attempt,
        })
    }

    pub const fn request(&self) -> &SnmpGetSpec {
        &self.request
    }

    pub const fn schedule(&self) -> PollSchedule {
        self.schedule
    }

    pub const fn attempt(&self) -> AttemptConfig {
        self.attempt
    }
}

#[derive(Clone, Debug)]
pub struct SnmpSetScenarioSpec {
    pub(super) request: SnmpSetSpec,
    pub(super) schedule: PollSchedule,
    pub(super) attempt: AttemptConfig,
}

impl SnmpSetScenarioSpec {
    pub fn try_new(
        request: SnmpSetSpec,
        schedule: PollSchedule,
        attempt: AttemptConfig,
    ) -> Result<Self, ScenarioSpecError> {
        schedule.validate()?;
        Ok(Self {
            request,
            schedule,
            attempt,
        })
    }

    pub const fn request(&self) -> &SnmpSetSpec {
        &self.request
    }

    pub const fn schedule(&self) -> PollSchedule {
        self.schedule
    }

    pub const fn attempt(&self) -> AttemptConfig {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[test]
    fn get_and_set_specs_validate_schedules_independently_of_attempt_budget() {
        let get =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let set = SnmpSetSpec::from_raw(
            "127.0.0.1".to_string(),
            161,
            None,
            "private".to_string(),
            None,
            vec![],
        )
        .unwrap();
        for duration in [
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_secs(3),
        ] {
            for schedule in [
                PollSchedule::FixedRate { period: duration },
                PollSchedule::FixedDelay { delay: duration },
            ] {
                let results = [
                    SnmpGetScenarioSpec::try_new(
                        get.clone(),
                        schedule,
                        AttemptConfig::try_new(Duration::from_secs(1), 1, Duration::from_secs(1))
                            .unwrap(),
                    )
                    .map(|_| ()),
                    SnmpSetScenarioSpec::try_new(
                        set.clone(),
                        schedule,
                        AttemptConfig::try_new(Duration::from_secs(1), 1, Duration::from_secs(1))
                            .unwrap(),
                    )
                    .map(|_| ()),
                ];
                for result in results {
                    if duration.is_zero() {
                        assert!(matches!(
                            result,
                            Err(ScenarioSpecError::ScheduleDurationMustBePositive)
                        ));
                    } else {
                        assert!(result.is_ok());
                    }
                }
            }
        }
    }
}
