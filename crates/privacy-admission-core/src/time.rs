#[cfg(test)]
mod tests {
    use std::time::Duration;

    use std::sync::Arc;

    use crate::{Clock, ManualClock, MonotonicClock, ReleasePolicy, ReleasePolicyError, Timestamp};

    #[test]
    fn monotonic_clock_observations_never_decrease() {
        // Given: a process-relative monotonic clock.
        let clock = MonotonicClock::new();

        // When: it is observed repeatedly.
        let observations = (0..32).map(|_| clock.now()).collect::<Vec<_>>();

        // Then: every observation is at least its predecessor.
        assert!(observations.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn monotonic_clock_clone_shares_the_same_baseline() {
        // Given: a process-relative monotonic clock.
        let clock = MonotonicClock::new();

        // When: the clock is cloned.
        let cloned = clock.clone();

        // Then: both clocks retain the exact same baseline allocation.
        assert!(Arc::ptr_eq(&clock.baseline, &cloned.baseline));
    }

    #[test]
    fn timestamp_converts_duration_and_rejects_unrepresentable_nanoseconds() {
        // Given: durations at the representable boundary and beyond it.
        let representable = Duration::from_nanos(u64::MAX);
        let oversized = Duration::new(u64::MAX, 999_999_999);

        // When: they become nanosecond timestamps.
        let converted = Timestamp::from_duration(representable).expect("u64 nanoseconds fit");
        let rejected = Timestamp::from_duration(oversized);

        // Then: only the representable duration is accepted.
        assert_eq!(converted, Timestamp(u64::MAX));
        assert!(rejected.is_err());
    }

    #[test]
    fn manual_clock_advances_with_checked_timestamp_arithmetic() {
        // Given: a manual clock near the timestamp limit.
        let mut clock = ManualClock::new(Timestamp(u64::MAX - 2));

        // When: it advances once within range and once beyond it.
        clock
            .advance(Duration::from_nanos(2))
            .expect("advance remains representable");
        let overflow = clock.advance(Duration::from_nanos(1));

        // Then: the valid advance is observable and overflow is rejected.
        assert_eq!(clock.now(), Timestamp(u64::MAX));
        assert!(overflow.is_err());
    }

    #[test]
    fn release_policy_rejects_zero_epoch_and_inverted_bounds() {
        // Given: invalid release-policy inputs.
        let zero_epoch = ReleasePolicy::new(
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_nanos(2),
        );
        let inverted_bounds = ReleasePolicy::new(
            Duration::from_nanos(10),
            Duration::from_nanos(3),
            Duration::from_nanos(2),
        );

        // When / Then: construction reports the specific validation errors.
        assert!(matches!(zero_epoch, Err(ReleasePolicyError::ZeroEpoch)));
        assert!(matches!(
            inverted_bounds,
            Err(ReleasePolicyError::MinimumExceedsMaximum)
        ));
    }

    #[test]
    fn release_policy_rejects_unrepresentable_minimum_and_maximum() {
        // Given: each release bound is oversized without making the bounds inverted.
        let oversized_minimum = Duration::new(u64::MAX, 1);
        let oversized_maximum = Duration::new(u64::MAX, 2);

        // When: each oversized bound is validated independently.
        let minimum_result = ReleasePolicy::new(
            Duration::from_nanos(1),
            oversized_minimum,
            oversized_maximum,
        );
        let maximum_result = ReleasePolicy::new(
            Duration::from_nanos(1),
            Duration::from_nanos(1),
            oversized_maximum,
        );

        // Then: the field-specific timestamp errors are returned.
        assert_eq!(
            minimum_result,
            Err(ReleasePolicyError::MinimumOutOfRange(
                crate::TimestampError::DurationOutOfRange
            ))
        );
        assert_eq!(
            maximum_result,
            Err(ReleasePolicyError::MaximumOutOfRange(
                crate::TimestampError::DurationOutOfRange
            ))
        );
    }

    #[test]
    fn release_policy_keeps_exact_epoch_and_caps_to_maximum_delay() {
        // Given: policies that exercise the epoch and maximum-delay paths.
        let exact_epoch = ReleasePolicy::new(
            Duration::from_nanos(10),
            Duration::from_nanos(5),
            Duration::from_nanos(25),
        )
        .expect("valid policy");
        let capped = ReleasePolicy::new(
            Duration::from_nanos(10),
            Duration::from_nanos(5),
            Duration::from_nanos(11),
        )
        .expect("valid policy");

        // When: accepted timestamps are scheduled.
        let exact_release = exact_epoch
            .release_at(Timestamp(15))
            .expect("release remains representable");
        let capped_release = capped
            .release_at(Timestamp(17))
            .expect("release remains representable");

        // Then: an exact epoch is unchanged and the maximum delay wins when earlier.
        assert_eq!(exact_release, Timestamp(20));
        assert_eq!(capped_release, Timestamp(28));
    }
}
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A timestamp measured as nanoseconds from a caller-defined epoch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Convert a duration from the epoch into a representable timestamp.
    pub fn from_duration(duration: Duration) -> Result<Self, TimestampError> {
        u64::try_from(duration.as_nanos())
            .map(Self)
            .map_err(|_| TimestampError::DurationOutOfRange)
    }

    /// Return the timestamp as nanoseconds from the caller-defined epoch.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Add a duration while rejecting values beyond the timestamp range.
    pub fn checked_add(self, duration: Duration) -> Result<Self, TimestampError> {
        let duration = Self::from_duration(duration)?;
        self.0
            .checked_add(duration.0)
            .map(Self)
            .ok_or(TimestampError::Overflow)
    }

    /// Return the duration elapsed since an earlier timestamp.
    pub fn checked_duration_since(self, earlier: Self) -> Result<Duration, TimestampError> {
        self.0
            .checked_sub(earlier.0)
            .map(Duration::from_nanos)
            .ok_or(TimestampError::Underflow)
    }
}

/// Errors from converting or computing timestamps.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum TimestampError {
    /// A duration cannot be represented by a nanosecond timestamp.
    #[error("duration exceeds the nanosecond timestamp range")]
    DurationOutOfRange,
    /// A timestamp addition exceeded the nanosecond timestamp range.
    #[error("timestamp addition overflowed")]
    Overflow,
    /// A timestamp subtraction preceded the caller-defined epoch.
    #[error("timestamp subtraction underflowed")]
    Underflow,
}

/// Source of the current admission-scheduling time.
pub trait Clock {
    /// Return the current timestamp.
    fn now(&self) -> Timestamp;
}

/// A cloneable clock measured from one shared process-relative baseline.
#[derive(Clone, Debug)]
pub struct MonotonicClock {
    baseline: Arc<Instant>,
}

impl MonotonicClock {
    /// Create a clock whose zero timestamp is the current monotonic instant.
    pub fn new() -> Self {
        Self {
            baseline: Arc::new(Instant::now()),
        }
    }

    /// Convert a timestamp from this clock into its monotonic instant.
    pub fn instant_at(&self, timestamp: Timestamp) -> Option<Instant> {
        self.baseline
            .checked_add(Duration::from_nanos(timestamp.as_nanos()))
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        match u64::try_from(self.baseline.elapsed().as_nanos()) {
            Ok(nanoseconds) => Timestamp(nanoseconds),
            Err(_) => Timestamp(u64::MAX),
        }
    }
}

/// A deterministic clock controlled by its caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManualClock {
    now: Timestamp,
}

impl ManualClock {
    /// Create a manual clock at the supplied timestamp.
    pub const fn new(now: Timestamp) -> Self {
        Self { now }
    }

    /// Set the current timestamp.
    pub fn set(&mut self, now: Timestamp) {
        self.now = now;
    }

    /// Advance the current timestamp by a checked duration.
    pub fn advance(&mut self, duration: Duration) -> Result<(), TimestampError> {
        self.now = self.now.checked_add(duration)?;
        Ok(())
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        self.now
    }
}

/// Validated bounds for releasing an accepted admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleasePolicy {
    epoch: u64,
    minimum: Duration,
    maximum: Duration,
}

impl ReleasePolicy {
    /// Validate policy durations and create a release policy.
    pub fn new(
        epoch: Duration,
        minimum: Duration,
        maximum: Duration,
    ) -> Result<Self, ReleasePolicyError> {
        let epoch = Timestamp::from_duration(epoch).map_err(ReleasePolicyError::EpochOutOfRange)?;
        if epoch.0 == 0 {
            return Err(ReleasePolicyError::ZeroEpoch);
        }
        Timestamp::from_duration(minimum).map_err(ReleasePolicyError::MinimumOutOfRange)?;
        Timestamp::from_duration(maximum).map_err(ReleasePolicyError::MaximumOutOfRange)?;
        if minimum > maximum {
            return Err(ReleasePolicyError::MinimumExceedsMaximum);
        }

        Ok(Self {
            epoch: epoch.0,
            minimum,
            maximum,
        })
    }

    /// Round a timestamp up to the next epoch, preserving exact boundaries.
    pub fn next_epoch(&self, timestamp: Timestamp) -> Result<Timestamp, ReleasePolicyError> {
        let remainder = timestamp.0 % self.epoch;
        let adjustment = if remainder == 0 {
            0
        } else {
            self.epoch - remainder
        };
        timestamp
            .checked_add(Duration::from_nanos(adjustment))
            .map_err(ReleasePolicyError::Timestamp)
    }

    /// Schedule an acceptance at the earliest permitted epoch, capped by maximum delay.
    pub fn release_at(&self, accepted: Timestamp) -> Result<Timestamp, ReleasePolicyError> {
        let earliest = accepted
            .checked_add(self.minimum)
            .map_err(ReleasePolicyError::Timestamp)?;
        let latest = accepted
            .checked_add(self.maximum)
            .map_err(ReleasePolicyError::Timestamp)?;
        let next_epoch = self.next_epoch(earliest)?;

        Ok(next_epoch.min(latest))
    }
}

impl Default for ReleasePolicy {
    fn default() -> Self {
        Self {
            epoch: 60_000_000_000,
            minimum: Duration::from_secs(5 * 60),
            maximum: Duration::from_secs(10 * 60),
        }
    }
}

/// Errors from validating or evaluating a release policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ReleasePolicyError {
    /// The epoch duration cannot be represented as nanoseconds.
    #[error("epoch duration exceeds the nanosecond timestamp range")]
    EpochOutOfRange(TimestampError),
    /// The minimum release delay cannot be represented as nanoseconds.
    #[error("minimum release delay exceeds the nanosecond timestamp range")]
    MinimumOutOfRange(TimestampError),
    /// The maximum release delay cannot be represented as nanoseconds.
    #[error("maximum release delay exceeds the nanosecond timestamp range")]
    MaximumOutOfRange(TimestampError),
    /// The epoch duration is zero.
    #[error("epoch duration must be non-zero")]
    ZeroEpoch,
    /// The minimum delay exceeds the maximum delay.
    #[error("minimum release delay exceeds maximum release delay")]
    MinimumExceedsMaximum,
    /// Timestamp arithmetic failed while evaluating the policy.
    #[error(transparent)]
    Timestamp(TimestampError),
}
