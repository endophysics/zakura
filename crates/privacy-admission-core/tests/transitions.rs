//! Single-record state-transition behavior.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionStateLabel, ReasonCode,
    Timestamp, TransitionOutcome,
};

fn admitted_core() -> (AdmissionCore<CountingClock>, CountingClock) {
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("fixture admission succeeds");
    (core, clock)
}

#[test]
fn refresh_atomically_moves_all_due_records_and_returns_sorted_ids() {
    // Given: admissions with two due schedules and one later schedule.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(9), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(13));
    core.admit(AdmissionId(3), AdmissionOrigin::Development)
        .expect("admission succeeds");
    clock.set(Timestamp(27));
    core.admit(AdmissionId(7), AdmissionOrigin::Development)
        .expect("admission succeeds");
    assert_eq!(
        core.embargoed_ids(),
        vec![AdmissionId(3), AdmissionId(7), AdmissionId(9)]
    );
    assert!(core.eligible_ids().is_empty());
    clock.set(Timestamp(30));
    let calls = clock.calls();

    // When: one set-wide refresh observes the clock.
    let transitioned = core.refresh().expect("refresh succeeds");

    // Then: every and only due record moves atomically in identifier order.
    assert_eq!(clock.calls(), calls + 1);
    assert_eq!(transitioned, vec![AdmissionId(3), AdmissionId(9)]);
    assert_eq!(core.embargoed_ids(), vec![AdmissionId(7)]);
    assert_eq!(core.eligible_ids(), vec![AdmissionId(3), AdmissionId(9)]);
}

#[test]
fn refresh_without_due_records_commits_the_successful_clock_observation() {
    // Given: one admission before its scheduled release.
    let (mut core, clock) = admitted_core();
    clock.set(Timestamp(19));
    let calls = clock.calls();

    // When: set-wide refresh finds no due records.
    let transitioned = core.refresh().expect("refresh succeeds");

    // Then: no membership changes, one read occurs, and the marker advances.
    assert!(transitioned.is_empty());
    assert_eq!(clock.calls(), calls + 1);
    assert_eq!(core.embargoed_ids(), vec![AdmissionId(7)]);
    assert!(core.eligible_ids().is_empty());
    clock.set(Timestamp(18));
    assert!(matches!(
        core.refresh(),
        Err(AdmissionError::ClockRollback {
            last_observed: Timestamp(19),
            ..
        })
    ));
}

#[test]
fn reject_stamps_terminal_time_and_reason() {
    // Given: a due eligible admission and a validated policy reason.
    let (mut core, clock) = admitted_core();
    clock.set(Timestamp(20));
    core.refresh().expect("refresh succeeds");
    clock.set(Timestamp(22));
    let reason = ReasonCode::try_from("policy").expect("reason is valid");

    // When: policy rejects the admission.
    let outcome = core
        .reject(AdmissionId(7), reason.clone())
        .expect("rejection succeeds");

    // Then: rejection is terminal and retains the immutable schedule.
    let TransitionOutcome::Updated(view) = outcome else {
        panic!("nonterminal rejection must update");
    };
    assert_eq!(view.state.label(), AdmissionStateLabel::Rejected);
    assert_eq!(view.state.terminal_at(), Some(Timestamp(22)));
    assert_eq!(view.state.reason(), Some(&reason));
    assert_eq!(view.state.scheduled_release_at(), Timestamp(20));
}

#[test]
fn remove_stamps_terminal_time_and_reason() {
    // Given: an embargoed admission and a removal reason.
    let (mut core, clock) = admitted_core();
    clock.set(Timestamp(15));
    let reason = ReasonCode::try_from("operator").expect("reason is valid");

    // When: the admission is removed.
    let outcome = core
        .remove(AdmissionId(7), reason.clone())
        .expect("removal succeeds");

    // Then: removal records the terminal facts.
    let TransitionOutcome::Updated(view) = outcome else {
        panic!("nonterminal removal must update");
    };
    assert_eq!(view.state.label(), AdmissionStateLabel::Removed);
    assert_eq!(view.state.terminal_at(), Some(Timestamp(15)));
    assert_eq!(view.state.reason(), Some(&reason));
}

#[test]
fn terminal_state_absorbs_later_mutation_without_clock_read() {
    // Given: a rejected admission and a clock moved backward afterward.
    let (mut core, clock) = admitted_core();
    let reason = ReasonCode::try_from("policy").expect("reason is valid");
    core.reject(AdmissionId(7), reason.clone())
        .expect("rejection succeeds");
    let calls = clock.calls();
    clock.set(Timestamp(1));

    // When: a later removal targets the terminal admission.
    let outcome = core
        .remove(AdmissionId(7), reason)
        .expect("terminal state absorbs mutation");

    // Then: the terminal record is returned unchanged without observing time.
    let TransitionOutcome::Existing(view) = outcome else {
        panic!("terminal mutation must return existing");
    };
    assert_eq!(view.state.label(), AdmissionStateLabel::Rejected);
    assert_eq!(clock.calls(), calls);
}

#[test]
fn refresh_rollback_leaves_state_unchanged() {
    // Given: one due and one non-due admission behind the last clock marker.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(9), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(27));
    core.admit(AdmissionId(7), AdmissionOrigin::Development)
        .expect("admission succeeds");
    let before = core.snapshot();
    let calls = clock.calls();
    clock.set(Timestamp(20));

    // When: refresh observes the rollback.
    let error = core.refresh().expect_err("rollback is rejected");

    // Then: one clock read changes neither membership nor the clock marker.
    assert_eq!(
        error,
        AdmissionError::ClockRollback {
            observed: Timestamp(20),
            last_observed: Timestamp(27),
        }
    );
    assert_eq!(clock.calls(), calls + 1);
    assert_eq!(core.snapshot(), before);
    assert_eq!(core.embargoed_ids(), vec![AdmissionId(7), AdmissionId(9)]);
    assert!(core.eligible_ids().is_empty());
    clock.set(Timestamp(26));
    assert!(matches!(
        core.refresh(),
        Err(AdmissionError::ClockRollback {
            last_observed: Timestamp(27),
            ..
        })
    ));
}

#[test]
fn unknown_ids_return_typed_errors_for_terminal_transitions() {
    // Given: an empty core.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock, policy());
    let reason = ReasonCode::try_from("policy").expect("reason is valid");

    // When: each state transition targets an unknown identifier.
    let reject = core.reject(AdmissionId(9), reason.clone());
    let remove = core.remove(AdmissionId(9), reason);

    // Then: every transition reports the same typed missing-record boundary.
    let expected = AdmissionError::UnknownAdmission {
        admission_id: AdmissionId(9),
    };
    assert_eq!(reject, Err(expected.clone()));
    assert_eq!(remove, Err(expected));
}
