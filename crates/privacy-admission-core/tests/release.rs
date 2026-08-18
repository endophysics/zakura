//! Atomic due-batch release behavior.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionStateLabel, BatchId,
    ReleaseOutcome, Timestamp,
};

#[test]
fn release_due_releases_embargoed_directly_in_deterministic_order() {
    // Given: two due admissions with one later admission outside the due set.
    let clock = CountingClock::new(Timestamp(13));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(9), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(13));
    core.admit(AdmissionId(3), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(27));
    core.admit(AdmissionId(1), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(37));
    core.admit(AdmissionId(0), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(40));
    let calls = clock.calls();

    // When: release runs without a prior refresh.
    let outcome = core.release_due().expect("release succeeds");

    // Then: one batch starts at zero and is ordered by schedule then identifier.
    let ReleaseOutcome::Released {
        batch_id,
        admissions,
    } = outcome
    else {
        panic!("due admissions must form a batch");
    };
    assert_eq!(clock.calls(), calls + 1);
    assert_eq!(batch_id, BatchId(0));
    assert_eq!(
        admissions
            .iter()
            .map(|view| view.admission_id)
            .collect::<Vec<_>>(),
        vec![AdmissionId(3), AdmissionId(9), AdmissionId(1)]
    );
    assert!(admissions
        .iter()
        .all(|view| view.state.label() == AdmissionStateLabel::Released));
    assert_eq!(
        core.get(AdmissionId(0))
            .expect("record exists")
            .state
            .label(),
        AdmissionStateLabel::Embargoed
    );
}

#[test]
fn release_due_includes_previously_eligible_admissions() {
    // Given: an admission made eligible by refresh.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    core.refresh().expect("refresh succeeds");
    clock.set(Timestamp(21));

    // When: the complete due set is released.
    let outcome = core.release_due().expect("release succeeds");

    // Then: the eligible admission joins the batch.
    assert!(matches!(
        outcome,
        ReleaseOutcome::Released {
            batch_id: BatchId(0),
            ref admissions,
        } if admissions.len() == 1
            && admissions[0].state.label() == AdmissionStateLabel::Released
    ));
}

#[test]
fn no_due_advances_clock_marker_without_consuming_batch() {
    // Given: one admission before its deadline.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(19));

    // When: release observes no due admissions.
    let outcome = core.release_due().expect("empty release succeeds");

    // Then: no batch is consumed, so the eventual due release still uses zero.
    assert_eq!(outcome, ReleaseOutcome::NoDue);
    clock.set(Timestamp(20));
    let released = core.release_due().expect("due release succeeds");
    assert!(matches!(
        released,
        ReleaseOutcome::Released {
            batch_id: BatchId(0),
            ..
        }
    ));
}

#[test]
fn release_rollback_reads_once_and_leaves_state_unchanged() {
    // Given: an admission and a rolled-back clock.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    let before = core.snapshot();
    clock.set(Timestamp(11));
    let calls = clock.calls();

    // When: release observes the rollback.
    let error = core.release_due().expect_err("rollback is rejected");

    // Then: one observation reports the rollback without mutation.
    assert_eq!(clock.calls(), calls + 1);
    assert_eq!(
        error,
        AdmissionError::ClockRollback {
            observed: Timestamp(11),
            last_observed: Timestamp(12),
        }
    );
    assert_eq!(core.snapshot(), before);
}

#[test]
fn repeated_release_does_not_release_terminal_admissions_again() {
    // Given: an admission released in batch zero.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    core.release_due().expect("first release succeeds");

    // When: release is repeated at the same accepted observation.
    let repeated = core.release_due().expect("repeat succeeds");

    // Then: the released terminal is not batched again.
    assert_eq!(repeated, ReleaseOutcome::NoDue);
}
