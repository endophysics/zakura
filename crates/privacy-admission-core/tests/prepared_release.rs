//! Prepared due-batch release behavior.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionStateLabel, BatchId,
    ReasonCode, ReleaseOutcome, Timestamp,
};

#[test]
fn preparation_selects_due_set_without_transitioning_or_consuming_batch() {
    // Given: one due embargoed admission and one eligible admission.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(2), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    core.refresh().expect("refresh succeeds");
    core.admit(AdmissionId(1), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(30));
    let before = core.snapshot();

    // When: the complete due set is prepared.
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admissions are due");

    // Then: callers can inspect the batch without lifecycle or batch mutation.
    assert_eq!(prepared.batch_id(), BatchId(0));
    assert_eq!(prepared.admission_ids(), &[AdmissionId(2), AdmissionId(1)]);
    assert_eq!(core.snapshot(), before);
    assert!(core.eligible_ids().iter().all(|id| *id == AdmissionId(2)));
}

#[test]
fn dropping_preparation_leaves_due_records_and_batch_number_available() {
    // Given: a due admission whose first preparation is abandoned.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    let abandoned = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admission is due");
    drop(abandoned);

    // When: the due set is prepared and committed later.
    let prepared = core
        .prepare_release()
        .expect("second preparation succeeds")
        .expect("admission remains due");
    let outcome = core.commit_release(prepared).expect("commit succeeds");

    // Then: the abandoned preparation consumed no batch identifier.
    assert!(matches!(
        outcome,
        ReleaseOutcome::Released {
            batch_id: BatchId(0),
            ..
        }
    ));
}

#[test]
fn commit_atomically_releases_the_whole_prepared_set() {
    // Given: two due admissions in one preparation.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    for id in [AdmissionId(4), AdmissionId(5)] {
        core.admit(id, AdmissionOrigin::PrivateGateway)
            .expect("admission succeeds");
    }
    clock.set(Timestamp(20));
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admissions are due");

    // When: the preparation is committed.
    let outcome = core.commit_release(prepared).expect("commit succeeds");

    // Then: every prepared admission transitions in exactly one batch.
    let ReleaseOutcome::Released {
        batch_id,
        admissions,
    } = outcome
    else {
        panic!("prepared admissions must be released");
    };
    assert_eq!(batch_id, BatchId(0));
    assert_eq!(admissions.len(), 2);
    assert!(admissions.iter().all(|view| {
        view.state.label() == AdmissionStateLabel::Released
            && view.state.batch_id() == Some(BatchId(0))
    }));
}

#[test]
fn preparation_is_stale_after_rejecting_a_member() {
    // Given: a preparation containing one due admission.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admission is due");
    core.reject(AdmissionId(7), reason())
        .expect("rejection succeeds");
    let before = core.snapshot();

    // When: the outdated preparation is committed.
    let result = core.commit_release(prepared);

    // Then: the stale error leaves the rejected state unchanged.
    assert_eq!(result, Err(AdmissionError::StalePreparedRelease));
    assert_eq!(core.snapshot(), before);
}

#[test]
fn preparation_is_stale_after_removing_a_member() {
    // Given: a preparation containing one due admission.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admission is due");
    core.remove(AdmissionId(7), reason())
        .expect("removal succeeds");
    let before = core.snapshot();

    // When: the outdated preparation is committed.
    let result = core.commit_release(prepared);

    // Then: the stale error leaves the removed state unchanged.
    assert_eq!(result, Err(AdmissionError::StalePreparedRelease));
    assert_eq!(core.snapshot(), before);
}

#[test]
fn committing_one_of_two_outstanding_preparations_stales_the_other() {
    // Given: two preparations for the same due set and prospective batch.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    let first = core
        .prepare_release()
        .expect("first preparation succeeds")
        .expect("admission is due");
    let second = core
        .prepare_release()
        .expect("second preparation succeeds")
        .expect("admission is due");
    assert_eq!(first.batch_id(), second.batch_id());
    core.commit_release(first).expect("first commit succeeds");
    let before = core.snapshot();

    // When: the other outstanding preparation is committed.
    let result = core.commit_release(second);

    // Then: the completed batch makes the duplicate preparation stale.
    assert_eq!(result, Err(AdmissionError::StalePreparedRelease));
    assert_eq!(core.snapshot(), before);
}

#[test]
fn prepare_rollback_reads_once_and_is_atomic() {
    // Given: an admission and a rolled-back clock.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    let before = core.snapshot();
    clock.set(Timestamp(11));
    let calls = clock.calls();

    // When: preparation observes the rollback.
    let error = core.prepare_release().expect_err("rollback is rejected");

    // Then: one observation reports rollback without mutation.
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
fn commit_does_not_read_clock_again() {
    // Given: a due preparation made with one clock observation.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admission is due");
    let calls = clock.calls();
    clock.set(Timestamp(0));

    // When: the prepared batch is committed after the clock changes.
    core.commit_release(prepared).expect("commit succeeds");

    // Then: commit uses the captured observation without another clock read.
    assert_eq!(clock.calls(), calls);
}

#[test]
fn preparation_orders_by_schedule_then_admission_identifier() {
    // Given: due admissions with differing and matching schedules.
    let clock = CountingClock::new(Timestamp(13));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(9), AdmissionOrigin::Development)
        .expect("admission succeeds");
    core.admit(AdmissionId(3), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(27));
    core.admit(AdmissionId(1), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(40));

    // When: the due set is prepared.
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admissions are due");

    // Then: schedule is primary and admission identifier breaks ties.
    assert_eq!(
        prepared.admission_ids(),
        &[AdmissionId(3), AdmissionId(9), AdmissionId(1)]
    );
}

fn reason() -> ReasonCode {
    ReasonCode::try_from("prepared_stale").expect("reason is valid")
}
