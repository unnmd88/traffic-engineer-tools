use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::time::Duration;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CoordinationError {
    #[error("cycle must contain at least one phase")]
    EmptyCycle,
    #[error("phase {id} must have a positive duration")]
    NonPositivePhaseDuration { id: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase {
    pub id: u32,
    pub duration: Duration,
}

impl Phase {
    pub fn new(id: u32, duration: Duration) -> Self {
        Self { id, duration }
    }
}

#[derive(Debug, Clone)]
pub struct Cycle {
    phases: Vec<Phase>,
}

impl Cycle {
    pub fn try_new(phases: Vec<Phase>) -> Result<Self, CoordinationError> {
        if phases.is_empty() {
            return Err(CoordinationError::EmptyCycle);
        }
        for phase in &phases {
            if phase.duration.is_zero() {
                return Err(CoordinationError::NonPositivePhaseDuration { id: phase.id });
            }
        }
        Ok(Self { phases })
    }

    pub fn total(&self) -> Duration {
        self.phases.iter().map(|p| p.duration).sum()
    }

    pub fn phase_at(&self, now: DateTime<Utc>, epoch: DateTime<Utc>) -> PhaseState {
        let total_ms = self.total().as_millis() as i64;
        let elapsed_ms = now.signed_duration_since(epoch).num_milliseconds();
        let pos = elapsed_ms.rem_euclid(total_ms);

        let mut acc: i64 = 0;
        for phase in &self.phases {
            let dur_ms = phase.duration.as_millis() as i64;
            if pos < acc + dur_ms {
                return PhaseState {
                    phase_id: phase.id,
                    elapsed: Duration::from_millis((pos - acc) as u64),
                    remaining: Duration::from_millis((acc + dur_ms - pos) as u64),
                };
            }
            acc += dur_ms;
        }

        unreachable!("phases cover total duration");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseState {
    pub phase_id: u32,
    pub elapsed: Duration,
    pub remaining: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Epoch {
    UnixEpoch,
    StartOfDay,
    Fixed(DateTime<Utc>),
}

impl Epoch {
    pub fn resolve(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Epoch::UnixEpoch => DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"),
            Epoch::StartOfDay => now
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("midnight")
                .and_utc(),
            Epoch::Fixed(dt) => *dt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn cycle() -> Cycle {
        Cycle::try_new(vec![
            Phase::new(1, Duration::from_secs(30)),
            Phase::new(2, Duration::from_secs(40)),
            Phase::new(3, Duration::from_secs(50)),
        ])
        .unwrap()
    }

    #[test]
    fn rejects_empty_cycle() {
        assert!(matches!(
            Cycle::try_new(vec![]),
            Err(CoordinationError::EmptyCycle)
        ));
    }

    #[test]
    fn rejects_zero_duration() {
        let err = Cycle::try_new(vec![Phase::new(1, Duration::ZERO)]).unwrap_err();
        assert!(matches!(
            err,
            CoordinationError::NonPositivePhaseDuration { id: 1 }
        ));
    }

    #[test]
    fn total_is_sum_of_phases() {
        assert_eq!(cycle().total(), Duration::from_secs(120));
    }

    #[test]
    fn phase_at_selects_first_phase_at_epoch() {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let st = cycle().phase_at(epoch, epoch);
        assert_eq!(st.phase_id, 1);
        assert_eq!(st.elapsed, Duration::ZERO);
        assert_eq!(st.remaining, Duration::from_secs(30));
    }

    #[test]
    fn phase_at_selects_second_phase() {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let now = epoch + ChronoDuration::seconds(45);
        let st = cycle().phase_at(now, epoch);
        assert_eq!(st.phase_id, 2);
        assert_eq!(st.elapsed, Duration::from_secs(15));
        assert_eq!(st.remaining, Duration::from_secs(25));
    }

    #[test]
    fn phase_at_wraps_around_cycle() {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let now = epoch + ChronoDuration::seconds(130);
        let st = cycle().phase_at(now, epoch);
        assert_eq!(st.phase_id, 1);
        assert_eq!(st.elapsed, Duration::from_secs(10));
    }

    #[test]
    fn phase_at_before_epoch_wraps() {
        let epoch = DateTime::<Utc>::from_timestamp(1000, 0).unwrap();
        let now = epoch - ChronoDuration::seconds(20);
        let st = cycle().phase_at(now, epoch);
        assert_eq!(st.phase_id, 3);
        assert_eq!(st.elapsed, Duration::from_secs(30));
        assert_eq!(st.remaining, Duration::from_secs(20));
    }
}
