//! Admission, retry, and monotonic-clock behavior.

mod common;

use std::time::Duration;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionOutcome,
    AdmissionStateLabel, ReleasePolicy, ReleasePolicyError, Timestamp, TimestampError,
};

#[test]
fn new_admission_observes_once_and_persists_embargoed_schedule() {
    // Given: an empty core at a deterministic instant.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());

    // When: a private-gateway admission is accepted.
    let outcome = core
        .admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("new admission succeeds");

    // Then: one observation determines the immutable embargo schedule.
    let AdmissionOutcome::Accepted(view) = outcome else {
        panic!("new admission must be reported as accepted");
    };
    assert_eq!(clock.calls(), 1);
    assert_eq!(view.state.label(), AdmissionStateLabel::Embargoed);
    assert_eq!(view.state.accepted_at(), Timestamp(12));
    assert_eq!(view.state.scheduled_release_at(), Timestamp(20));
}

#[test]
fn same_origin_retry_returns_existing_before_reading_clock() {
    // Given: an admission whose clock is later moved backward.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("initial admission succeeds");
    clock.set(Timestamp(1));

    // When: the same origin retries the same identifier.
    let outcome = core
        .admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("same-origin retry is idempotent");

    // Then: the original record is returned without another clock read.
    assert!(matches!(outcome, AdmissionOutcome::Existing(_)));
    assert_eq!(clock.calls(), 1);
}

#[test]
fn conflicting_origin_errors_before_reading_clock_or_mutating() {
    // Given: an existing private-gateway admission.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("initial admission succeeds");
    let before = core.snapshot();

    // When: a different origin claims the same identifier.
    let error = core
        .admit(AdmissionId(7), AdmissionOrigin::Development)
        .expect_err("conflicting origin is rejected");

    // Then: neither time nor state changes.
    assert_eq!(
        error,
        AdmissionError::ConflictingOrigin {
            admission_id: AdmissionId(7),
            existing: AdmissionOrigin::PrivateGateway,
            requested: AdmissionOrigin::Development,
        }
    );
    assert_eq!(clock.calls(), 1);
    assert_eq!(core.snapshot(), before);
}

#[test]
fn clock_rollback_rejects_new_admission_without_mutation() {
    // Given: a core that previously accepted a later observation.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("initial admission succeeds");
    let before = core.snapshot();
    clock.set(Timestamp(11));

    // When: a distinct admission observes rolled-back time.
    let error = core
        .admit(AdmissionId(8), AdmissionOrigin::Development)
        .expect_err("clock rollback is rejected");

    // Then: the observation is typed and no record is added.
    assert_eq!(
        error,
        AdmissionError::ClockRollback {
            observed: Timestamp(11),
            last_observed: Timestamp(12),
        }
    );
    assert_eq!(core.snapshot(), before);
    assert!(core.get(AdmissionId(8)).is_none());
}

#[test]
fn schedule_overflow_leaves_clock_marker_and_records_unchanged() {
    // Given: a policy whose minimum delay overflows at the observed timestamp.
    let clock = CountingClock::new(Timestamp(u64::MAX));
    let policy = ReleasePolicy::new(
        Duration::from_nanos(1),
        Duration::from_nanos(1),
        Duration::from_nanos(1),
    )
    .expect("test policy is valid");
    let mut core = AdmissionCore::new(clock.clone(), policy);

    // When: admission scheduling cannot be represented.
    let error = core
        .admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect_err("overflow is rejected");

    // Then: no record or clock marker is committed.
    assert_eq!(
        error,
        AdmissionError::Schedule(ReleasePolicyError::Timestamp(TimestampError::Overflow))
    );
    assert!(core.snapshot().admissions.is_empty());
    clock.set(Timestamp(u64::MAX - 1));
    assert!(matches!(
        core.admit(AdmissionId(8), AdmissionOrigin::Development),
        Ok(AdmissionOutcome::Accepted(_))
    ));
}
