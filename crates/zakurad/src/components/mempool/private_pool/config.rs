use std::time::Duration;

use privacy_admission_core::{ReleasePolicy, ReleasePolicyError};
use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;

/// Default maximum number of verified private transactions.
pub const DEFAULT_MAX_PRIVATE_TRANSACTIONS: usize = 1_000;

/// Default maximum serialized bytes retained by the private pool.
pub const DEFAULT_MAX_PRIVATE_SERIALIZED_BYTES: usize = 16 * 1024 * 1024;

/// Default fixed release epoch.
pub const DEFAULT_RELEASE_EPOCH: Duration = Duration::from_secs(60);

/// Default minimum private retention duration.
pub const DEFAULT_MINIMUM_RELEASE_DELAY: Duration = Duration::from_secs(5 * 60);

/// Default maximum private retention duration.
pub const DEFAULT_MAXIMUM_RELEASE_DELAY: Duration = Duration::from_secs(10 * 60);

/// Validated private release durations and policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateReleaseConfig {
    epoch: Duration,
    minimum: Duration,
    maximum: Duration,
    policy: ReleasePolicy,
}

impl PrivateReleaseConfig {
    /// Validate private release durations.
    pub fn new(
        epoch: Duration,
        minimum: Duration,
        maximum: Duration,
    ) -> Result<Self, PrivatePoolConfigError> {
        if minimum.is_zero() {
            return Err(PrivatePoolConfigError::ZeroMinimumReleaseDelay);
        }
        if maximum.is_zero() {
            return Err(PrivatePoolConfigError::ZeroMaximumReleaseDelay);
        }
        Ok(Self {
            epoch,
            minimum,
            maximum,
            policy: ReleasePolicy::new(epoch, minimum, maximum)?,
        })
    }
}

impl Default for PrivateReleaseConfig {
    fn default() -> Self {
        Self {
            epoch: DEFAULT_RELEASE_EPOCH,
            minimum: DEFAULT_MINIMUM_RELEASE_DELAY,
            maximum: DEFAULT_MAXIMUM_RELEASE_DELAY,
            policy: ReleasePolicy::default(),
        }
    }
}

/// Independent private-pool capacity limits.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "RawPrivatePoolConfig")]
pub struct PrivatePoolConfig {
    /// Maximum number of retained transactions.
    max_transactions: usize,
    /// Maximum sum of retained transactions' serialized sizes.
    max_serialized_bytes: usize,
    release_epoch: Duration,
    minimum_release_delay: Duration,
    maximum_release_delay: Duration,
    release_policy: ReleasePolicy,
}

impl PrivatePoolConfig {
    /// Validate and construct independent private-pool limits.
    pub const fn new(
        max_transactions: usize,
        max_serialized_bytes: usize,
        release: PrivateReleaseConfig,
    ) -> Result<Self, PrivatePoolConfigError> {
        if max_transactions == 0 {
            return Err(PrivatePoolConfigError::ZeroTransactions);
        }
        if max_serialized_bytes == 0 {
            return Err(PrivatePoolConfigError::ZeroSerializedBytes);
        }
        Ok(Self {
            max_transactions,
            max_serialized_bytes,
            release_epoch: release.epoch,
            minimum_release_delay: release.minimum,
            maximum_release_delay: release.maximum,
            release_policy: release.policy,
        })
    }

    /// Return the configured transaction-count limit.
    pub const fn max_transactions(self) -> usize {
        self.max_transactions
    }

    /// Return the configured serialized-byte limit.
    pub const fn max_serialized_bytes(self) -> usize {
        self.max_serialized_bytes
    }

    /// Return the release policy validated during configuration parsing.
    pub const fn release_policy(self) -> ReleasePolicy {
        self.release_policy
    }

    /// Return the configured fixed release epoch.
    pub const fn release_epoch(self) -> Duration {
        self.release_epoch
    }

    /// Return the configured minimum release delay.
    pub const fn minimum_release_delay(self) -> Duration {
        self.minimum_release_delay
    }

    /// Return the configured maximum release delay.
    pub const fn maximum_release_delay(self) -> Duration {
        self.maximum_release_delay
    }
}

impl Default for PrivatePoolConfig {
    fn default() -> Self {
        let release = PrivateReleaseConfig::default();
        Self {
            max_transactions: DEFAULT_MAX_PRIVATE_TRANSACTIONS,
            max_serialized_bytes: DEFAULT_MAX_PRIVATE_SERIALIZED_BYTES,
            release_epoch: release.epoch,
            minimum_release_delay: release.minimum,
            maximum_release_delay: release.maximum,
            release_policy: release.policy,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct RawPrivatePoolConfig {
    max_transactions: usize,
    max_serialized_bytes: usize,
    #[serde(with = "humantime_serde")]
    release_epoch: Duration,
    #[serde(with = "humantime_serde")]
    minimum_release_delay: Duration,
    #[serde(with = "humantime_serde")]
    maximum_release_delay: Duration,
}

impl Default for RawPrivatePoolConfig {
    fn default() -> Self {
        Self {
            max_transactions: DEFAULT_MAX_PRIVATE_TRANSACTIONS,
            max_serialized_bytes: DEFAULT_MAX_PRIVATE_SERIALIZED_BYTES,
            release_epoch: DEFAULT_RELEASE_EPOCH,
            minimum_release_delay: DEFAULT_MINIMUM_RELEASE_DELAY,
            maximum_release_delay: DEFAULT_MAXIMUM_RELEASE_DELAY,
        }
    }
}

impl TryFrom<RawPrivatePoolConfig> for PrivatePoolConfig {
    type Error = PrivatePoolConfigError;

    fn try_from(raw: RawPrivatePoolConfig) -> Result<Self, Self::Error> {
        let release = PrivateReleaseConfig::new(
            raw.release_epoch,
            raw.minimum_release_delay,
            raw.maximum_release_delay,
        )?;
        Self::new(raw.max_transactions, raw.max_serialized_bytes, release)
    }
}

impl Serialize for PrivatePoolConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RawPrivatePoolConfig {
            max_transactions: self.max_transactions,
            max_serialized_bytes: self.max_serialized_bytes,
            release_epoch: self.release_epoch,
            minimum_release_delay: self.minimum_release_delay,
            maximum_release_delay: self.maximum_release_delay,
        }
        .serialize(serializer)
    }
}

/// Invalid private-pool capacity configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PrivatePoolConfigError {
    /// The transaction-count limit is zero.
    #[error("private transaction limit must be nonzero")]
    ZeroTransactions,
    /// The serialized-byte limit is zero.
    #[error("private serialized byte limit must be nonzero")]
    ZeroSerializedBytes,
    /// The minimum release delay is zero.
    #[error("minimum private release delay must be nonzero")]
    ZeroMinimumReleaseDelay,
    /// The maximum release delay is zero.
    #[error("maximum private release delay must be nonzero")]
    ZeroMaximumReleaseDelay,
    /// The release-policy durations are invalid.
    #[error(transparent)]
    ReleasePolicy(#[from] ReleasePolicyError),
}
