use std::time::Duration;

use super::*;
use crate::components::mempool::private_pool::{PrivatePoolConfig, PrivateReleaseConfig};

const MAX_CONNECTIONS_PER_IP: usize = 4;
const PEERSET_INITIAL_TARGET_SIZE: usize = 75;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

mod diversity;

fn config(
    max_transactions: usize,
    max_serialized_bytes: usize,
    epoch: Duration,
    minimum: Duration,
    maximum: Duration,
) -> TestResult<PrivatePoolConfig> {
    let release = PrivateReleaseConfig::new(epoch, minimum, maximum)?;
    Ok(PrivatePoolConfig::new(
        max_transactions,
        max_serialized_bytes,
        release,
    )?)
}

fn baseline_config() -> TestResult<PrivatePoolConfig> {
    config(
        1_000,
        16 * 1024 * 1024,
        Duration::new(60, 123),
        Duration::new(300, 456),
        Duration::new(600, 789),
    )
}

fn hash(config: PrivatePoolConfig) -> TestResult<String> {
    Ok(
        policy(config, MAX_CONNECTIONS_PER_IP, PEERSET_INITIAL_TARGET_SIZE)?
            .hash()
            .to_hex(),
    )
}

fn policy(
    config: PrivatePoolConfig,
    max_connections_per_ip: usize,
    peerset_initial_target_size: usize,
) -> TestResult<OperatorPrivacyPolicy> {
    Ok(OperatorPrivacyPolicy::new(
        config,
        max_connections_per_ip,
        peerset_initial_target_size,
    )?)
}

#[test]
fn equal_full_policies_have_equal_hashes() -> TestResult {
    // Given: independently constructed policies with equal complete inputs.
    let first = policy(
        baseline_config()?,
        MAX_CONNECTIONS_PER_IP,
        PEERSET_INITIAL_TARGET_SIZE,
    )?;
    let second = policy(
        baseline_config()?,
        MAX_CONNECTIONS_PER_IP,
        PEERSET_INITIAL_TARGET_SIZE,
    )?;

    // When: both complete policies are hashed.
    let first_hash = first.hash();
    let second_hash = second.hash();

    // Then: construction identity does not affect the digest.
    assert_eq!(first_hash, second_hash);
    Ok(())
}

#[test]
fn every_included_config_setting_changes_the_hash() -> TestResult {
    // Given: the baseline policy and one valid change to each included setting.
    let baseline = hash(baseline_config()?)?;
    let variants = [
        config(
            1_001,
            16 * 1024 * 1024,
            Duration::new(60, 123),
            Duration::new(300, 456),
            Duration::new(600, 789),
        )?,
        config(
            1_000,
            16 * 1024 * 1024 + 1,
            Duration::new(60, 123),
            Duration::new(300, 456),
            Duration::new(600, 789),
        )?,
        config(
            1_000,
            16 * 1024 * 1024,
            Duration::new(61, 123),
            Duration::new(300, 456),
            Duration::new(600, 789),
        )?,
        config(
            1_000,
            16 * 1024 * 1024,
            Duration::new(60, 123),
            Duration::new(301, 456),
            Duration::new(600, 789),
        )?,
        config(
            1_000,
            16 * 1024 * 1024,
            Duration::new(60, 123),
            Duration::new(300, 456),
            Duration::new(601, 789),
        )?,
    ];

    // When: each variant is projected and hashed.
    // Then: every included setting is digest-sensitive.
    for variant in variants {
        assert_ne!(baseline, hash(variant)?);
    }
    Ok(())
}

#[test]
fn canonical_encoding_has_stable_known_digest() -> TestResult {
    // Given: a policy containing nonzero values in every canonical numeric field.
    let policy = policy(
        baseline_config()?,
        MAX_CONNECTIONS_PER_IP,
        PEERSET_INITIAL_TARGET_SIZE,
    )?;

    // When: the canonical bytes are hashed.
    let digest = policy.hash().to_hex();

    // Then: the digest remains stable across implementations.
    assert_eq!(
        digest,
        "29a1f5fdb33f5e3da1be6edd92eea5a9e3b04c59d39d0ae6043eecfb538e2552"
    );
    Ok(())
}

#[test]
fn projection_api_accepts_validated_pool_and_peer_diversity_controls() -> TestResult {
    // Given: the public projection constructor signature.
    let constructor: fn(
        PrivatePoolConfig,
        usize,
        usize,
    ) -> Result<OperatorPrivacyPolicy, OperatorPrivacyPolicyError> = OperatorPrivacyPolicy::new;

    // When: validated pool and peer-diversity configuration are projected.
    let result = constructor(
        baseline_config()?,
        MAX_CONNECTIONS_PER_IP,
        PEERSET_INITIAL_TARGET_SIZE,
    );

    // Then: the typed policy is constructible without build or runtime metadata.
    assert!(result.is_ok());
    Ok(())
}
