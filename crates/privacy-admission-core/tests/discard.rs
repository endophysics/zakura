//! Compensation behavior for externally unretained admissions.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionStateLabel, ReasonCode,
    Timestamp,
};

#[test]
fn discard_removes_embargoed_and_eligible_records_without_reading_clock() {
    // Given: one eligible and one embargoed admission.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(1), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(20));
    core.refresh().expect("refresh succeeds");
    clock.set(Timestamp(21));
    core.admit(AdmissionId(2), AdmissionOrigin::Development)
        .expect("admission succeeds");
    let calls = clock.calls();

    // When: both uncommitted records are discarded.
    let eligible = core
        .discard_uncommitted(AdmissionId(1))
        .expect("eligible discard succeeds");
    let embargoed = core
        .discard_uncommitted(AdmissionId(2))
        .expect("embargoed discard succeeds");

    // Then: both owned outcomes describe removed records without observing time.
    assert_eq!(eligible.state.label(), AdmissionStateLabel::Eligible);
    assert_eq!(embargoed.state.label(), AdmissionStateLabel::Embargoed);
    assert_eq!(clock.calls(), calls);
    assert!(core.snapshot().admissions.is_empty());
}

#[test]
fn discard_rejects_unknown_and_terminal_records_without_mutation() {
    // Given: released, rejected, and removed records plus one unknown identifier.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    for id in [AdmissionId(1), AdmissionId(2), AdmissionId(3)] {
        core.admit(id, AdmissionOrigin::PrivateGateway)
            .expect("admission succeeds");
    }
    let reason = ReasonCode::try_from("terminal").expect("reason is valid");
    core.reject(AdmissionId(2), reason.clone())
        .expect("rejection succeeds");
    core.remove(AdmissionId(3), reason)
        .expect("removal succeeds");
    clock.set(Timestamp(20));
    core.release_due().expect("release succeeds");
    let before = core.snapshot();
    let calls = clock.calls();

    // When: discard targets unknown and terminal records.
    let unknown = core.discard_uncommitted(AdmissionId(9));
    let released = core.discard_uncommitted(AdmissionId(1));
    let rejected = core.discard_uncommitted(AdmissionId(2));
    let removed = core.discard_uncommitted(AdmissionId(3));

    // Then: typed errors preserve every record and avoid clock reads.
    assert_eq!(
        unknown,
        Err(AdmissionError::UnknownAdmission {
            admission_id: AdmissionId(9),
        })
    );
    for (result, admission_id, state) in [
        (released, AdmissionId(1), AdmissionStateLabel::Released),
        (rejected, AdmissionId(2), AdmissionStateLabel::Rejected),
        (removed, AdmissionId(3), AdmissionStateLabel::Removed),
    ] {
        assert_eq!(
            result,
            Err(AdmissionError::TerminalAdmission {
                admission_id,
                state,
            })
        );
    }
    assert_eq!(clock.calls(), calls);
    assert_eq!(core.snapshot(), before);
}

#[test]
fn discard_makes_an_outstanding_preparation_stale() {
    // Given: a prepared due batch containing two admissions.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    for id in [AdmissionId(1), AdmissionId(2)] {
        core.admit(id, AdmissionOrigin::PrivateGateway)
            .expect("admission succeeds");
    }
    clock.set(Timestamp(20));
    let prepared = core
        .prepare_release()
        .expect("preparation succeeds")
        .expect("admissions are due");
    core.discard_uncommitted(AdmissionId(1))
        .expect("discard succeeds");
    let before = core.snapshot();

    // When: the outdated preparation is committed.
    let result = core.commit_release(prepared);

    // Then: validation rejects it without releasing the remaining admission.
    assert_eq!(result, Err(AdmissionError::StalePreparedRelease));
    assert_eq!(core.snapshot(), before);
    assert_eq!(
        core.get(AdmissionId(2))
            .expect("remaining admission exists")
            .state
            .label(),
        AdmissionStateLabel::Embargoed
    );
}

#[test]
fn compensated_admission_leaves_no_terminal_record() {
    // Given: an admission whose external payload could not be retained.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock, policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");

    // When: admission compensation discards the record.
    let discarded = core
        .discard_uncommitted(AdmissionId(7))
        .expect("discard succeeds");

    // Then: the outcome remains inspectable but no lifecycle record remains.
    assert_eq!(discarded.admission_id, AdmissionId(7));
    assert_eq!(discarded.state.label(), AdmissionStateLabel::Embargoed);
    assert!(core.get(AdmissionId(7)).is_none());
    assert!(core.snapshot().admissions.is_empty());
}
