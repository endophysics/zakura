use serde::Serialize;

use super::OperatorPrivacyPolicy;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct OperatorPrivacyPolicySummary {
    pub(crate) policy_version: u32,
    pub(crate) policy_hash: String,
    pub(crate) max_private_transactions: u64,
    pub(crate) max_private_serialized_bytes: u64,
    pub(crate) release_timing: &'static str,
    pub(crate) release_epoch_seconds: u64,
    pub(crate) release_epoch_nanoseconds: u32,
    pub(crate) minimum_release_delay_seconds: u64,
    pub(crate) minimum_release_delay_nanoseconds: u32,
    pub(crate) maximum_release_delay_seconds: u64,
    pub(crate) maximum_release_delay_nanoseconds: u32,
    pub(crate) egress: &'static str,
    pub(crate) peer_diversity: &'static str,
    pub(crate) max_connections_per_ip: u64,
    pub(crate) peerset_initial_target_size: u64,
}

impl OperatorPrivacyPolicy {
    pub(crate) fn summary(&self) -> OperatorPrivacyPolicySummary {
        let release_epoch = self.release_epoch();
        let minimum_release_delay = self.minimum_release_delay();
        let maximum_release_delay = self.maximum_release_delay();
        OperatorPrivacyPolicySummary {
            policy_version: self.version(),
            policy_hash: self.hash().to_hex(),
            max_private_transactions: self.max_transactions(),
            max_private_serialized_bytes: self.max_serialized_bytes(),
            release_timing: self.release_timing(),
            release_epoch_seconds: release_epoch.as_secs(),
            release_epoch_nanoseconds: release_epoch.subsec_nanos(),
            minimum_release_delay_seconds: minimum_release_delay.as_secs(),
            minimum_release_delay_nanoseconds: minimum_release_delay.subsec_nanos(),
            maximum_release_delay_seconds: maximum_release_delay.as_secs(),
            maximum_release_delay_nanoseconds: maximum_release_delay.subsec_nanos(),
            egress: self.egress(),
            peer_diversity: self.peer_diversity(),
            max_connections_per_ip: self.max_connections_per_ip(),
            peerset_initial_target_size: self.peerset_initial_target_size(),
        }
    }
}
