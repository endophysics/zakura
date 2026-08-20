use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicyRecord<'a> {
    pub version: u32,
    pub hash: &'a str,
    pub release_timing: &'a str,
    pub epoch: Duration,
    pub minimum: Duration,
    pub maximum: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolCounts {
    pub private: usize,
    pub zakura_public: usize,
    pub observer_public: usize,
}

pub fn format_policy_records(policy: PolicyRecord<'_>) -> [String; 6] {
    [
        format!("policy_version={}", policy.version),
        format!("policy_hash={}", policy.hash),
        format!("release_timing={}", policy.release_timing),
        format!(
            "release_epoch_seconds={} release_epoch_nanoseconds={}",
            policy.epoch.as_secs(),
            policy.epoch.subsec_nanos()
        ),
        format!(
            "minimum_release_delay_seconds={} minimum_release_delay_nanoseconds={}",
            policy.minimum.as_secs(),
            policy.minimum.subsec_nanos()
        ),
        format!(
            "maximum_release_delay_seconds={} maximum_release_delay_nanoseconds={}",
            policy.maximum.as_secs(),
            policy.maximum.subsec_nanos()
        ),
    ]
}

pub fn format_timeline_record(event: &str, counts: MempoolCounts) -> String {
    format!(
        "timeline_event={event} private_mempool_count={} zakura_public_mempool_count={} observer_public_mempool_count={}",
        counts.private, counts.zakura_public, counts.observer_public
    )
}

pub const fn p2p_observer_record() -> &'static str {
    "p2p_observer_event=zcashd_observed_public_release"
}

pub const fn completion_record() -> &'static str {
    "timeline_event=inspection_complete"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_records_have_stable_machine_readable_fields() {
        // Given: a policy projection with subsecond timing components.
        let policy = PolicyRecord {
            version: 1,
            hash: "abc123",
            release_timing: "fixed_epoch",
            epoch: Duration::new(0, 250_000_000),
            minimum: Duration::new(5, 1),
            maximum: Duration::new(6, 2),
        };

        // When: policy records are formatted.
        let records = format_policy_records(policy);

        // Then: every stable field and exact duration component is explicit.
        assert_eq!(
            records,
            [
                "policy_version=1",
                "policy_hash=abc123",
                "release_timing=fixed_epoch",
                "release_epoch_seconds=0 release_epoch_nanoseconds=250000000",
                "minimum_release_delay_seconds=5 minimum_release_delay_nanoseconds=1",
                "maximum_release_delay_seconds=6 maximum_release_delay_nanoseconds=2",
            ]
        );
    }

    #[test]
    fn timeline_record_contains_only_event_and_numeric_counts() {
        // Given: queried aggregate counts at one causal stage.
        let counts = MempoolCounts {
            private: 1,
            zakura_public: 2,
            observer_public: 3,
        };

        // When: a timeline record is formatted.
        let record = format_timeline_record("private_retry_existing", counts);

        // Then: the stable record contains the event and all three counts.
        assert_eq!(record, "timeline_event=private_retry_existing private_mempool_count=1 zakura_public_mempool_count=2 observer_public_mempool_count=3");
    }

    #[test]
    fn observer_and_completion_records_are_stable_and_identifier_free() {
        // Given / When: terminal records are requested.
        let observer = p2p_observer_record();
        let complete = completion_record();

        // Then: both records are fixed tokens with no identity field.
        assert_eq!(
            observer,
            "p2p_observer_event=zcashd_observed_public_release"
        );
        assert_eq!(complete, "timeline_event=inspection_complete");
        assert!(!observer.contains("txid"));
        assert!(!complete.contains("txid"));
    }
}
