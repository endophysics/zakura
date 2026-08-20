use serde_json::json;

use super::*;
use crate::components::mempool::private_pool::PrivatePoolConfig;

#[test]
fn structured_summary_preserves_the_operator_policy_contract(
) -> Result<(), Box<dyn std::error::Error>> {
    // Given: a validated policy and distinct peer-diversity controls.
    let policy = OperatorPrivacyPolicy::new(PrivatePoolConfig::default(), 4, 75)?;

    // When: the policy crosses the structured startup-record boundary.
    let summary = policy.summary();
    let structured = serde_json::to_value(summary)?;

    // Then: stable fields retain exact integer values and machine-readable policy labels.
    assert_eq!(
        structured,
        json!({
            "policy_version": 1,
            "policy_hash": "f2ffa95e587aa9e78ff371ef491e7e49149e6eb8df3dc13ed4eabd9a10c37525",
            "max_private_transactions": 1_000,
            "max_private_serialized_bytes": 16_777_216,
            "release_timing": "fixed_epoch",
            "release_epoch_seconds": 60,
            "release_epoch_nanoseconds": 0,
            "minimum_release_delay_seconds": 300,
            "minimum_release_delay_nanoseconds": 0,
            "maximum_release_delay_seconds": 600,
            "maximum_release_delay_nanoseconds": 0,
            "egress": "common_randomized_peer_set",
            "peer_diversity": "per_ip_and_target_size",
            "max_connections_per_ip": 4,
            "peerset_initial_target_size": 75
        })
    );
    Ok(())
}

#[test]
fn structured_summary_excludes_private_and_build_identity() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a structured summary projected only from validated policy and diversity controls.
    let policy = OperatorPrivacyPolicy::new(PrivatePoolConfig::default(), 1, 50)?;

    // When: its field set crosses the structured boundary.
    let structured = serde_json::to_value(policy.summary())?;

    // Then: admission, transaction, node, peer, and build identities are absent.
    for excluded in [
        "admission_id",
        "transaction_id",
        "node_id",
        "peer_id",
        "build_version",
        "git_commit",
    ] {
        assert!(structured.get(excluded).is_none(), "unexpected {excluded}");
    }
    Ok(())
}
