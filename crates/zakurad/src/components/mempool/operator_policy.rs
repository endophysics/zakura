use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;

use super::private_pool::PrivatePoolConfig;

mod summary;

const DOMAIN: &[u8] = b"zakura.operator-privacy-policy";
const POLICY_VERSION: u32 = 1;
const ENABLED: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReleaseTimingPolicy {
    FixedEpoch,
}

impl ReleaseTimingPolicy {
    const fn discriminant(self) -> u8 {
        match self {
            Self::FixedEpoch => 1,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::FixedEpoch => "fixed_epoch",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EgressPolicy {
    CommonRandomizedPeerSet,
}

impl EgressPolicy {
    const fn discriminant(self) -> u8 {
        match self {
            Self::CommonRandomizedPeerSet => 1,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::CommonRandomizedPeerSet => "common_randomized_peer_set",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerDiversityPolicy {
    PerIpAndTargetSize,
}

impl PeerDiversityPolicy {
    const fn discriminant(self) -> u8 {
        match self {
            Self::PerIpAndTargetSize => 1,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::PerIpAndTargetSize => "per_ip_and_target_size",
        }
    }
}

/// The compile-time privacy behavior projected from validated operator configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorPrivacyPolicy {
    max_transactions: u64,
    max_serialized_bytes: u64,
    release_epoch: Duration,
    minimum_release_delay: Duration,
    maximum_release_delay: Duration,
    release_timing: ReleaseTimingPolicy,
    egress: EgressPolicy,
    peer_diversity: PeerDiversityPolicy,
    max_connections_per_ip: u64,
    peerset_initial_target_size: u64,
}

impl OperatorPrivacyPolicy {
    /// Project validated private-pool and peer-diversity configuration.
    ///
    /// # Errors
    ///
    /// Returns a field-specific error when a platform-sized input does not fit its
    /// canonical unsigned 64-bit field.
    pub fn new(
        config: PrivatePoolConfig,
        max_connections_per_ip: usize,
        peerset_initial_target_size: usize,
    ) -> Result<Self, OperatorPrivacyPolicyError> {
        Ok(Self {
            max_transactions: checked_size(
                config.max_transactions(),
                OperatorPrivacyPolicyError::MaxTransactionsOutOfRange,
            )?,
            max_serialized_bytes: checked_size(
                config.max_serialized_bytes(),
                OperatorPrivacyPolicyError::MaxSerializedBytesOutOfRange,
            )?,
            release_epoch: config.release_epoch(),
            minimum_release_delay: config.minimum_release_delay(),
            maximum_release_delay: config.maximum_release_delay(),
            release_timing: ReleaseTimingPolicy::FixedEpoch,
            egress: EgressPolicy::CommonRandomizedPeerSet,
            peer_diversity: PeerDiversityPolicy::PerIpAndTargetSize,
            max_connections_per_ip: checked_size(
                max_connections_per_ip,
                OperatorPrivacyPolicyError::MaxConnectionsPerIpOutOfRange,
            )?,
            peerset_initial_target_size: checked_size(
                peerset_initial_target_size,
                OperatorPrivacyPolicyError::PeersetInitialTargetSizeOutOfRange,
            )?,
        })
    }

    /// Return the canonical policy version.
    pub const fn version(&self) -> u32 {
        POLICY_VERSION
    }

    /// Return the bounded private transaction capacity.
    pub const fn max_transactions(&self) -> u64 {
        self.max_transactions
    }

    /// Return the bounded private serialized-byte capacity.
    pub const fn max_serialized_bytes(&self) -> u64 {
        self.max_serialized_bytes
    }

    /// Return the configured fixed release epoch.
    pub const fn release_epoch(&self) -> Duration {
        self.release_epoch
    }

    /// Return the configured minimum release delay.
    pub const fn minimum_release_delay(&self) -> Duration {
        self.minimum_release_delay
    }

    /// Return the configured maximum release delay.
    pub const fn maximum_release_delay(&self) -> Duration {
        self.maximum_release_delay
    }

    /// Return the stable release-timing policy label.
    pub const fn release_timing(&self) -> &'static str {
        self.release_timing.label()
    }

    /// Return the stable logical-egress policy label.
    pub const fn egress(&self) -> &'static str {
        self.egress.label()
    }

    /// Return the stable peer-diversity policy label.
    pub const fn peer_diversity(&self) -> &'static str {
        self.peer_diversity.label()
    }

    /// Return the configured per-IP peer connection limit.
    pub const fn max_connections_per_ip(&self) -> u64 {
        self.max_connections_per_ip
    }

    /// Return the configured initial peer-set target size.
    pub const fn peerset_initial_target_size(&self) -> u64 {
        self.peerset_initial_target_size
    }

    /// Hash the canonical, domain- and version-separated policy encoding.
    pub fn hash(&self) -> OperatorPrivacyPolicyHash {
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN);
        hasher.update([0]);
        hasher.update(POLICY_VERSION.to_be_bytes());
        hasher.update([ENABLED]);
        hasher.update(self.max_transactions.to_be_bytes());
        hasher.update(self.max_serialized_bytes.to_be_bytes());
        hasher.update([self.release_timing.discriminant()]);
        update_duration(&mut hasher, self.release_epoch);
        update_duration(&mut hasher, self.minimum_release_delay);
        update_duration(&mut hasher, self.maximum_release_delay);
        hasher.update([self.egress.discriminant()]);
        hasher.update([self.peer_diversity.discriminant()]);
        hasher.update(self.max_connections_per_ip.to_be_bytes());
        hasher.update(self.peerset_initial_target_size.to_be_bytes());
        OperatorPrivacyPolicyHash(hasher.finalize().into())
    }
}

/// A SHA-256 digest of the canonical operator privacy policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorPrivacyPolicyHash([u8; 32]);

impl OperatorPrivacyPolicyHash {
    /// Encode the digest as lowercase hexadecimal.
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

/// A policy field cannot be represented by the canonical encoding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OperatorPrivacyPolicyError {
    /// The transaction limit does not fit the canonical unsigned 64-bit field.
    #[error("private transaction limit exceeds the operator policy representation")]
    MaxTransactionsOutOfRange,
    /// The byte limit does not fit the canonical unsigned 64-bit field.
    #[error("private serialized byte limit exceeds the operator policy representation")]
    MaxSerializedBytesOutOfRange,
    /// The per-IP peer limit does not fit the canonical unsigned 64-bit field.
    #[error("per-IP peer limit exceeds the operator policy representation")]
    MaxConnectionsPerIpOutOfRange,
    /// The peer-set target does not fit the canonical unsigned 64-bit field.
    #[error("peer-set target size exceeds the operator policy representation")]
    PeersetInitialTargetSizeOutOfRange,
}

fn checked_size<T>(
    value: T,
    error: OperatorPrivacyPolicyError,
) -> Result<u64, OperatorPrivacyPolicyError>
where
    T: TryInto<u64>,
{
    value.try_into().map_err(|_| error)
}

fn update_duration(hasher: &mut Sha256, duration: Duration) {
    hasher.update(duration.as_secs().to_be_bytes());
    hasher.update(duration.subsec_nanos().to_be_bytes());
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod summary_tests;
