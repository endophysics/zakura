use super::*;
use crate::components::mempool::private_pool::PrivatePoolConfigError;
use std::time::Duration;

#[test]
fn stats_expose_only_aggregate_capacity_and_state_totals() {
    // Given: one private verified record.
    let transaction = transactions(1).pop().expect("transaction");
    let expected_bytes = transaction.transaction.size();
    let mut pool = PrivateVerifiedPool::new(config(4, LARGE_LIMIT));
    pool.insert(record(40, transaction)).expect("record fits");

    // When: aggregate diagnostics are serialized.
    let stats = pool.stats();
    let json = serde_json::to_value(stats).expect("aggregate stats serialize");

    // Then: only capacity and coarse verified-state totals are present.
    assert_eq!(json["transaction_count"], 1);
    assert_eq!(json["serialized_bytes"], expected_bytes);
    assert_eq!(json["max_transactions"], 4);
    assert_eq!(json["max_serialized_bytes"], LARGE_LIMIT);
    assert_eq!(json["state_totals"]["verified"], 1);
    assert_eq!(json.as_object().expect("stats are an object").len(), 5);
}

#[test]
fn private_types_and_errors_format_without_sensitive_values() {
    // Given: a sentinel admission ID and transaction/hash values.
    const SENTINEL_ADMISSION_ID: u64 = 9_876_543_210_123_456_789;
    let transaction = transaction_with_output();
    let transaction_id = format!("{:?}", transaction.transaction.id());
    let mined_hash = format!("{:?}", transaction.transaction.id().mined_id());
    let candidate = record(SENTINEL_ADMISSION_ID, transaction);
    let candidate_debug = format!("{candidate:?}");
    let mut pool = PrivateVerifiedPool::new(config(1, LARGE_LIMIT));
    pool.insert(candidate).expect("record fits");
    let batch = pool
        .snapshot_batch(&[AdmissionId(SENTINEL_ADMISSION_ID)])
        .expect("record exists");

    // When: every diagnostic/error surface is formatted.
    let surfaces = [
        candidate_debug,
        format!("{pool:?}"),
        format!("{batch:?}"),
        format!("{:?}", PrivatePoolError::AdmissionIdConflict),
        PrivatePoolError::TransactionContextConflict.to_string(),
    ];

    // Then: no identity, hash, plaintext bytes, or per-admission metadata appears.
    for surface in surfaces {
        assert!(!surface.contains(&SENTINEL_ADMISSION_ID.to_string()));
        assert!(!surface.contains(&transaction_id));
        assert!(!surface.contains(&mined_hash));
    }
}

#[test]
fn zero_limits_fail_configuration_deserialization() {
    // Given: private-pool configuration with zero capacity.
    let zero_count = r#"{"max_transactions":0,"max_serialized_bytes":1}"#;
    let zero_bytes = r#"{"max_transactions":1,"max_serialized_bytes":0}"#;

    // When: each configuration crosses the serde boundary.
    let count_result = serde_json::from_str::<PrivatePoolConfig>(zero_count);
    let byte_result = serde_json::from_str::<PrivatePoolConfig>(zero_bytes);

    // Then: both invalid configurations are rejected.
    assert!(count_result.is_err());
    assert!(byte_result.is_err());
}

#[test]
fn invalid_release_durations_fail_configuration_deserialization() {
    // Given: zero and inverted private release durations.
    let zero_epoch = r#"{"release_epoch":"0s"}"#;
    let zero_minimum = r#"{"minimum_release_delay":"0s"}"#;
    let inverted = r#"{"minimum_release_delay":"3s","maximum_release_delay":"2s"}"#;

    // When: each configuration crosses the serde boundary.
    let epoch_result = serde_json::from_str::<PrivatePoolConfig>(zero_epoch);
    let minimum_result = serde_json::from_str::<PrivatePoolConfig>(zero_minimum);
    let inverted_result = serde_json::from_str::<PrivatePoolConfig>(inverted);

    // Then: every invalid release policy is rejected.
    assert!(epoch_result.is_err());
    assert!(minimum_result.is_err());
    assert!(inverted_result.is_err());
}

#[test]
fn unrepresentable_release_durations_fail_direct_private_release_configuration() {
    // Given: release bounds outside the timestamp nanosecond range.
    let oversized_minimum = Duration::new(u64::MAX, 1);
    let oversized_maximum = Duration::new(u64::MAX, 2);

    // When: each bound is validated independently.
    let minimum_result = PrivateReleaseConfig::new(
        Duration::from_nanos(1),
        oversized_minimum,
        oversized_maximum,
    );
    let maximum_result = PrivateReleaseConfig::new(
        Duration::from_nanos(1),
        Duration::from_nanos(1),
        oversized_maximum,
    );

    // Then: each field retains the typed release-policy error.
    assert_eq!(
        minimum_result,
        Err(PrivatePoolConfigError::ReleasePolicy(
            privacy_admission_core::ReleasePolicyError::MinimumOutOfRange(
                privacy_admission_core::TimestampError::DurationOutOfRange,
            ),
        ))
    );
    assert_eq!(
        maximum_result,
        Err(PrivatePoolConfigError::ReleasePolicy(
            privacy_admission_core::ReleasePolicyError::MaximumOutOfRange(
                privacy_admission_core::TimestampError::DurationOutOfRange,
            ),
        ))
    );
}

#[test]
fn unrepresentable_release_durations_fail_configuration_deserialization() {
    // Given: each oversized release bound at the serde boundary.
    let oversized_minimum = r#"{"minimum_release_delay":"18446744073709551615.000000001s","maximum_release_delay":"18446744073709551615.000000002s"}"#;
    let oversized_maximum = r#"{"minimum_release_delay":"1ns","maximum_release_delay":"18446744073709551615.000000002s"}"#;

    // When: each configuration crosses serde.
    let minimum_result = serde_json::from_str::<PrivatePoolConfig>(oversized_minimum);
    let maximum_result = serde_json::from_str::<PrivatePoolConfig>(oversized_maximum);

    // Then: neither unrepresentable bound can construct a pool config.
    assert!(minimum_result.is_err());
    assert!(maximum_result.is_err());
}

#[test]
fn default_release_durations_are_explicit_and_nonzero() {
    // Given: the production private-pool defaults.
    let config = PrivatePoolConfig::default();

    // When: the defaults cross the serialization boundary.
    let serialized = serde_json::to_value(config).expect("default config serializes");

    // Then: every release duration is explicit and nonzero.
    assert_eq!(serialized["release_epoch"], "1m");
    assert_eq!(config.release_epoch(), Duration::from_secs(60));
    assert_eq!(serialized["minimum_release_delay"], "5m");
    assert_eq!(serialized["maximum_release_delay"], "10m");
}
