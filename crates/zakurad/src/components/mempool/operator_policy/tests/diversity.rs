use super::*;

fn diversity_hash(
    max_connections_per_ip: usize,
    peerset_initial_target_size: usize,
) -> TestResult<String> {
    Ok(policy(
        baseline_config()?,
        max_connections_per_ip,
        peerset_initial_target_size,
    )?
    .hash()
    .to_hex())
}

#[test]
fn max_connections_per_ip_changes_the_hash() -> TestResult {
    // Given: policies differing only in the per-IP connection limit.
    let baseline = diversity_hash(MAX_CONNECTIONS_PER_IP, PEERSET_INITIAL_TARGET_SIZE)?;

    // When: the per-IP limit changes.
    let changed = diversity_hash(MAX_CONNECTIONS_PER_IP + 1, PEERSET_INITIAL_TARGET_SIZE)?;

    // Then: the canonical digest changes.
    assert_ne!(baseline, changed);
    Ok(())
}

#[test]
fn peerset_initial_target_size_changes_the_hash() -> TestResult {
    // Given: policies differing only in the initial peer-set target.
    let baseline = diversity_hash(MAX_CONNECTIONS_PER_IP, PEERSET_INITIAL_TARGET_SIZE)?;

    // When: the initial peer-set target changes.
    let changed = diversity_hash(MAX_CONNECTIONS_PER_IP, PEERSET_INITIAL_TARGET_SIZE + 1)?;

    // Then: the canonical digest changes.
    assert_ne!(baseline, changed);
    Ok(())
}

#[test]
fn unrepresentable_canonical_sizes_return_field_specific_errors() {
    // Given: capacities one larger than the canonical u64 representation.
    let oversized = u128::from(u64::MAX) + 1;

    // When: each capacity crosses the canonical size conversion.
    let transaction_result = checked_size(
        oversized,
        OperatorPrivacyPolicyError::MaxTransactionsOutOfRange,
    );
    let byte_result = checked_size(
        oversized,
        OperatorPrivacyPolicyError::MaxSerializedBytesOutOfRange,
    );
    let connections_result = checked_size(
        oversized,
        OperatorPrivacyPolicyError::MaxConnectionsPerIpOutOfRange,
    );
    let target_result = checked_size(
        oversized,
        OperatorPrivacyPolicyError::PeersetInitialTargetSizeOutOfRange,
    );

    // Then: each conversion fails with its field-specific typed error.
    assert_eq!(
        transaction_result,
        Err(OperatorPrivacyPolicyError::MaxTransactionsOutOfRange)
    );
    assert_eq!(
        byte_result,
        Err(OperatorPrivacyPolicyError::MaxSerializedBytesOutOfRange)
    );
    assert_eq!(
        connections_result,
        Err(OperatorPrivacyPolicyError::MaxConnectionsPerIpOutOfRange)
    );
    assert_eq!(
        target_result,
        Err(OperatorPrivacyPolicyError::PeersetInitialTargetSizeOutOfRange)
    );
}

#[test]
fn maximum_platform_capacities_are_safe_when_representable() -> TestResult {
    // Given: the largest capacities accepted by this platform's validated config.
    let config = config(
        usize::MAX,
        usize::MAX,
        Duration::from_nanos(1),
        Duration::from_nanos(1),
        Duration::from_nanos(2),
    )?;

    // When: the config is projected into the canonical policy.
    let result = OperatorPrivacyPolicy::new(config, usize::MAX, usize::MAX);

    // Then: 64-bit and smaller targets hash it safely; wider targets reject it.
    if usize::BITS <= u64::BITS {
        assert!(result.is_ok());
    } else {
        assert_eq!(
            result,
            Err(OperatorPrivacyPolicyError::MaxTransactionsOutOfRange)
        );
    }
    Ok(())
}
