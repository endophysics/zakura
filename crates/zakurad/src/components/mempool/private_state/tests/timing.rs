use super::*;

#[test]
fn release_timing_tracks_authoritative_core_transitions() {
    // Given: one retained admission and a timing subscriber created from private state.
    let (mut state, _) = due_state(1);
    let timing = state.release_timing();
    let expected_deadline = state.core.earliest_release_at();
    let mut storage = super::super::super::storage::Storage::new(&Default::default());
    assert_eq!(timing.deadline(), expected_deadline);

    // When: the due admission commits to public storage.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: the aggregate signal starts at the core deadline and clears after release.
    assert!(expected_deadline.is_some());
    assert_eq!(
        effects.outcome,
        PrivatePromotionOutcome::Promoted { count: 1 }
    );
    assert_eq!(timing.deadline(), None);
}

#[test]
fn retained_verifier_failure_is_recoverable_without_removal() {
    // Given: one retained record marked for revalidation.
    let (mut state, verified, context) = retained_state();
    let original_tip = state
        .retained_record(context.admission_id)
        .expect("record is retained")
        .verification_tip();
    state.begin_revalidation(
        VerificationTip::new(Some((Hash([2; 32]), Height(11)))),
        false,
    );

    // When: the retained transaction fails verification.
    state.fail(
        verified.transaction.id(),
        context,
        TransactionDownloadVerifyError::Cancelled,
    );

    // Then: ownership and metadata remain, the marker clears, and diagnostics recover.
    assert_eq!(
        state
            .retained_record(context.admission_id)
            .expect("record remains retained")
            .verification_tip(),
        original_tip
    );
    assert_eq!(state.diagnostics().recoverable_count, 1);
    assert!(state.snapshot_batch(&[context.admission_id]).is_ok());
}

#[test]
fn retained_success_replaces_record_without_changing_core_schedule() {
    // Given: one retained record marked for revalidation and its original core schedule.
    let (mut state, verified, context) = retained_state();
    let before = state.core.snapshot().admissions[0].clone();
    let current_tip = VerificationTip::new(Some((Hash([2; 32]), Height(11))));
    state.begin_revalidation(current_tip, false);

    // When: current-tip verification succeeds without a reservation.
    state.complete_verified(verified, Vec::new(), current_tip, context);

    // Then: the record is replaced while acceptance and release times remain unchanged.
    let after = state.core.snapshot().admissions[0].clone();
    assert_eq!(after.accepted_at_ns, before.accepted_at_ns);
    assert_eq!(
        after.scheduled_release_at_ns,
        before.scheduled_release_at_ns
    );
    assert_eq!(
        state
            .retained_record(context.admission_id)
            .expect("replacement remains retained")
            .verification_tip(),
        current_tip
    );
    assert!(state.snapshot_batch(&[context.admission_id]).is_ok());
}
