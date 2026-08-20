use std::time::Duration;

use privacy_admission_core::Timestamp;
use zakura_node_services::mempool::PrivateWindowAggregate;

use super::{PrivateTelemetry, PrivateTelemetryOutcome};

const EPOCH: Duration = Duration::from_nanos(10);

#[test]
fn diagnostics_are_empty_until_a_window_completes() {
    // Given: telemetry initialized in the first release-epoch window.
    let mut telemetry = PrivateTelemetry::new(Timestamp(0), EPOCH);

    // When: diagnostics are observed before its upper boundary.
    let completed = telemetry.completed_window(Timestamp(9));

    // Then: the current partial window is not exposed.
    assert_eq!(completed, None);
}

#[test]
fn outcomes_are_assigned_to_half_open_windows() {
    // Given: one outcome immediately before the first window boundary.
    let mut telemetry = PrivateTelemetry::new(Timestamp(0), EPOCH);
    telemetry.record(Timestamp(9), PrivateTelemetryOutcome::Promoted, 2);

    // When: an outcome occurs exactly at the boundary and another just after it.
    telemetry.record(Timestamp(10), PrivateTelemetryOutcome::Recoverable, 3);
    telemetry.record(Timestamp(11), PrivateTelemetryOutcome::Terminal, 4);

    // Then: the boundary published only the just-finished first window.
    assert_eq!(
        telemetry.completed_window(Timestamp(11)),
        Some(PrivateWindowAggregate {
            promoted: 2,
            recoverable: 0,
            terminal: 0,
        })
    );

    // When: the second window completes.
    let completed = telemetry.completed_window(Timestamp(20));

    // Then: exact-boundary and after-boundary outcomes belong to that second window.
    assert_eq!(
        completed,
        Some(PrivateWindowAggregate {
            promoted: 0,
            recoverable: 3,
            terminal: 4,
        })
    );
}

#[test]
fn skipped_windows_publish_the_immediately_preceding_zero_aggregate() {
    // Given: activity in the first window.
    let mut telemetry = PrivateTelemetry::new(Timestamp(0), EPOCH);
    telemetry.record(Timestamp(1), PrivateTelemetryOutcome::Promoted, 7);

    // When: diagnostics skip directly into the fourth window.
    let completed = telemetry.completed_window(Timestamp(30));

    // Then: only the immediately preceding, empty third window is published.
    assert_eq!(completed, Some(PrivateWindowAggregate::default()));
}

#[test]
fn outcome_counts_saturate_at_u64_max() {
    // Given: a current-window count at the integer limit.
    let mut telemetry = PrivateTelemetry::new(Timestamp(0), EPOCH);
    telemetry.record(Timestamp(1), PrivateTelemetryOutcome::Terminal, u64::MAX);

    // When: another terminal outcome is recorded in the same window.
    telemetry.record(Timestamp(2), PrivateTelemetryOutcome::Terminal, 1);

    // Then: rollover preserves saturation without wrapping.
    assert_eq!(
        telemetry.completed_window(Timestamp(10)),
        Some(PrivateWindowAggregate {
            promoted: 0,
            recoverable: 0,
            terminal: u64::MAX,
        })
    );
}
