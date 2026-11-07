use tokio::time::{Duration, Instant, sleep_until};

use super::ScenarioSpecError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollSchedule {
    FixedRate { period: Duration },
    FixedDelay { delay: Duration },
}

impl PollSchedule {
    pub const fn duration(self) -> Duration {
        match self {
            Self::FixedRate { period } => period,
            Self::FixedDelay { delay } => delay,
        }
    }

    pub(super) const fn validate(self) -> Result<(), ScenarioSpecError> {
        if self.duration().is_zero() {
            return Err(ScenarioSpecError::ScheduleDurationMustBePositive);
        }
        Ok(())
    }
}

pub(super) struct PollTimer {
    schedule: PollSchedule,
    origin: Instant,
    next_deadline: Instant,
}

impl PollTimer {
    pub(super) fn new(schedule: PollSchedule, now: Instant) -> Self {
        Self {
            schedule,
            origin: now,
            next_deadline: now + schedule.duration(),
        }
    }

    pub(super) async fn wait_next(&mut self) {
        let now = Instant::now();
        if let PollSchedule::FixedRate { period } = self.schedule
            && now > self.next_deadline
        {
            self.next_deadline = self.next_rate_deadline(period, now);
        }
        sleep_until(self.next_deadline).await;
    }

    pub(super) fn execution_finished(&mut self, now: Instant) {
        self.next_deadline = match self.schedule {
            PollSchedule::FixedRate { period } => {
                let next = self.next_deadline + period;
                if next < now {
                    self.next_rate_deadline(period, now)
                } else {
                    next
                }
            }
            PollSchedule::FixedDelay { delay } => now + delay,
        };
    }

    pub(super) fn resume(&mut self, now: Instant) {
        self.next_deadline = match self.schedule {
            PollSchedule::FixedRate { period } => self.next_rate_deadline(period, now),
            PollSchedule::FixedDelay { delay } => now + delay,
        };
    }

    fn next_rate_deadline(&self, period: Duration, now: Instant) -> Instant {
        let elapsed = now.duration_since(self.origin);
        let remaining = period.as_nanos() - elapsed.as_nanos() % period.as_nanos();
        let seconds = u64::try_from(remaining / 1_000_000_000)
            .expect("remaining seconds fit within the schedule duration");
        let nanos = u32::try_from(remaining % 1_000_000_000)
            .expect("fractional nanoseconds fit within u32");
        now + Duration::new(seconds, nanos)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tokio::time::{advance, sleep};

    use super::*;
    use crate::polling::{AttemptConfig, AttemptError, Pollable, poll};

    struct TimedRequest {
        duration: Duration,
    }

    #[async_trait]
    impl Pollable for TimedRequest {
        type Output = ();

        async fn poll(&self) -> Result<Self::Output, AttemptError> {
            sleep(self.duration).await;
            Ok(())
        }
    }

    async fn request_starts(schedule: PollSchedule, duration: Duration) -> Vec<Duration> {
        let origin = Instant::now();
        let mut timer = PollTimer::new(schedule, origin);
        let request = TimedRequest { duration };
        let attempt = AttemptConfig::try_new(Duration::from_secs(60), 0, Duration::ZERO).unwrap();
        let mut starts = Vec::new();
        for _ in 0..3 {
            timer.wait_next().await;
            starts.push(origin.elapsed());
            poll(&attempt, &request).await.unwrap();
            timer.execution_finished(Instant::now());
        }
        starts
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_rate_preserves_the_grid_across_requests() {
        let starts = request_starts(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            Duration::from_secs(3),
        )
        .await;
        assert_eq!(starts, [10, 20, 30].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_waits_after_each_completed_request() {
        let starts = request_starts(
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
            Duration::from_secs(3),
        )
        .await;
        assert_eq!(starts, [10, 23, 36].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_rate_skips_deadlines_missed_during_a_request() {
        let starts = request_starts(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            Duration::from_secs(25),
        )
        .await;
        assert_eq!(starts, [10, 40, 70].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_rate_skips_deadlines_missed_while_delivering_reports() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            origin,
        );
        timer.wait_next().await;
        advance(Duration::from_secs(3)).await;
        timer.execution_finished(Instant::now());
        advance(Duration::from_secs(13)).await;

        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_delay_runs_when_an_elapsed_delay_is_observed() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
            origin,
        );
        timer.wait_next().await;
        timer.execution_finished(Instant::now());
        advance(Duration::from_secs(25)).await;

        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_secs(35));
    }

    #[tokio::test(start_paused = true)]
    async fn resume_preserves_the_rate_grid_and_restarts_the_completion_delay() {
        let origin = Instant::now();
        let mut rate = PollTimer::new(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            origin,
        );
        let mut delay = PollTimer::new(
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
            origin,
        );
        advance(Duration::from_secs(27)).await;
        rate.resume(Instant::now());
        delay.resume(Instant::now());

        rate.wait_next().await;
        assert_eq!(origin.elapsed(), Duration::from_secs(30));
        delay.wait_next().await;
        assert_eq!(origin.elapsed(), Duration::from_secs(37));
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_on_a_grid_boundary_waits_for_the_next_period() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            origin,
        );
        advance(Duration::from_secs(20)).await;
        timer.resume(Instant::now());

        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn completing_on_a_grid_boundary_keeps_that_deadline_available() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            origin,
        );
        timer.wait_next().await;
        advance(Duration::from_secs(10)).await;
        timer.execution_finished(Instant::now());

        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_secs(20));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_rate_handles_large_gaps_and_fractional_periods() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedRate {
                period: Duration::from_millis(1500),
            },
            origin,
        );
        advance(Duration::from_secs(1_000_000)).await;

        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_millis(1_000_000_500));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_wait_does_not_postpone_the_deadline() {
        for schedule in [
            PollSchedule::FixedRate {
                period: Duration::from_secs(10),
            },
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(10),
            },
        ] {
            let origin = Instant::now();
            let mut timer = PollTimer::new(schedule, origin);
            let mut waiting = Box::pin(timer.wait_next());
            assert!(futures_util::poll!(waiting.as_mut()).is_pending());
            advance(Duration::from_secs(3)).await;
            drop(waiting);

            timer.wait_next().await;

            assert_eq!(origin.elapsed(), Duration::from_secs(10));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completion_delay_starts_after_all_timeout_attempts_and_retry_delays() {
        let origin = Instant::now();
        let mut timer = PollTimer::new(
            PollSchedule::FixedDelay {
                delay: Duration::from_secs(1),
            },
            origin,
        );
        let request = TimedRequest {
            duration: Duration::from_secs(10),
        };
        let attempt =
            AttemptConfig::try_new(Duration::from_secs(2), 2, Duration::from_secs(1)).unwrap();

        timer.wait_next().await;
        assert_eq!(origin.elapsed(), Duration::from_secs(1));
        let response = poll(&attempt, &request).await.unwrap();
        assert!(matches!(response, crate::polling::Response::NoResponse { attempts: 3, .. }));
        assert_eq!(origin.elapsed(), Duration::from_secs(9));
        timer.execution_finished(Instant::now());
        timer.wait_next().await;

        assert_eq!(origin.elapsed(), Duration::from_secs(10));
    }
}
