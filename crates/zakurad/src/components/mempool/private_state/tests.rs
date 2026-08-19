use std::{collections::HashSet, time::Duration};

use privacy_admission_core::AdmissionStateLabel;
use zakura_chain::{
    block::{Hash, Height},
    parameters::Network,
    transaction::VerifiedUnminedTx,
};
use zakura_node_services::mempool::{AdmissionId, AdmissionPolicy, PrivatePromotionOutcome};

use super::super::private_pool::PrivateReleaseConfig;
use super::super::TransactionDownloadVerifyError;
use super::*;

mod promotion;
mod terminal;
mod timing;

fn retained_state() -> (PrivateAdmissionState, VerifiedUnminedTx, AdmissionContext) {
    let verified = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("test vectors contain a transaction");
    let context = AdmissionContext {
        admission_id: AdmissionId(700),
        policy: AdmissionPolicy::FixedEpoch,
    };
    let mut state = PrivateAdmissionState::new(PrivatePoolConfig::default());
    let reservation = state
        .reserve(verified.transaction.clone(), context)
        .expect("private capacity is available");
    assert!(matches!(
        reservation,
        PrivateReservationOutcome::Accepted(_)
    ));
    state.complete_verified(
        verified.clone(),
        Vec::new(),
        VerificationTip::new(Some((Hash([1; 32]), Height(10)))),
        context,
    );
    (state, verified, context)
}

#[test]
fn active_exact_retry_remains_existing_without_mutation() {
    // Given: one active reservation for a transaction and context.
    let verified = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("test vectors contain a transaction");
    let context = AdmissionContext {
        admission_id: AdmissionId(700),
        policy: AdmissionPolicy::FixedEpoch,
    };
    let mut state = PrivateAdmissionState::new(PrivatePoolConfig::default());
    let first = state
        .reserve(verified.transaction.clone(), context)
        .expect("private capacity is available");
    let before = state.diagnostics();

    // When: the same active transaction and context are retried.
    let second = state
        .reserve(verified.transaction, context)
        .expect("exact active retry is existing");

    // Then: no second reservation or ownership mutation is created.
    assert!(matches!(first, PrivateReservationOutcome::Accepted(_)));
    assert!(matches!(second, PrivateReservationOutcome::Existing));
    assert_eq!(state.diagnostics(), before);
    assert_eq!(state.reservations.len(), 1);
}

fn due_state(count: usize) -> (PrivateAdmissionState, Vec<VerifiedUnminedTx>) {
    due_state_with_expiry(count, None)
}

fn due_state_with_expiry(
    count: usize,
    first_expiry: Option<Height>,
) -> (PrivateAdmissionState, Vec<VerifiedUnminedTx>) {
    let release = PrivateReleaseConfig::new(
        Duration::from_nanos(1),
        Duration::from_nanos(1),
        Duration::from_nanos(1),
    )
    .expect("test release policy is valid");
    let config = PrivatePoolConfig::new(count.max(1), usize::MAX, release)
        .expect("test private capacity is valid");
    let mut transactions = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .filter(|transaction| {
            transaction
                .transaction
                .transaction()
                .expiry_height()
                .is_some()
                && transaction
                    .transaction
                    .transaction()
                    .outputs()
                    .iter()
                    .all(|output| !output.is_dust())
        })
        .take(count)
        .collect::<Vec<_>>();
    assert_eq!(transactions.len(), count, "test vectors cover the batch");
    if let Some(expiry_height) = first_expiry {
        let mut transaction = transactions[0].transaction.transaction().as_ref().clone();
        *transaction.expiry_height_mut() = expiry_height;
        transactions[0].transaction = transaction.into();
    }
    let mut state = PrivateAdmissionState::new(config);
    for (index, transaction) in transactions.iter().enumerate() {
        let context = AdmissionContext {
            admission_id: AdmissionId(
                u64::try_from(index).expect("test batch length fits in admission IDs"),
            ),
            policy: AdmissionPolicy::FixedEpoch,
        };
        let reservation = state
            .reserve(transaction.transaction.clone(), context)
            .expect("private capacity is available");
        assert!(matches!(
            reservation,
            PrivateReservationOutcome::Accepted(_)
        ));
        state.complete_verified(
            transaction.clone(),
            Vec::new(),
            VerificationTip::new(Some((Hash([1; 32]), Height(10)))),
            context,
        );
    }
    std::thread::sleep(Duration::from_millis(1));
    (state, transactions)
}

#[test]
fn promotion_reports_no_due_without_mutation() {
    // Given: an empty private admission state and public storage.
    let mut state = PrivateAdmissionState::new(PrivatePoolConfig::default());
    let before = state.diagnostics();
    let mut storage = super::super::storage::Storage::new(&Default::default());

    // When: synchronous promotion observes no due admission.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: the aggregate outcome is NoDue and all owners remain unchanged.
    assert_eq!(effects.outcome, PrivatePromotionOutcome::NoDue);
    assert!(effects.accepted.is_empty());
    assert!(effects.evicted.is_empty());
    assert_eq!(state.diagnostics(), before);
    assert_eq!(storage.transaction_count(), 0);
}

#[test]
fn promotion_commits_the_complete_due_batch_in_deterministic_order() {
    // Given: three retained records whose common release deadline is due.
    let (mut state, transactions) = due_state(3);
    let expected_ids = transactions
        .iter()
        .map(|transaction| transaction.transaction.id())
        .collect::<HashSet<_>>();
    let mut storage = super::super::storage::Storage::new(&Default::default());

    // When: the exact complete due snapshot is promoted once.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: public, core, and private ownership commit as one complete batch.
    assert_eq!(
        effects.outcome,
        PrivatePromotionOutcome::Promoted { count: 3 }
    );
    assert_eq!(effects.accepted, expected_ids);
    assert!(effects.evicted.is_empty());
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), expected_ids);
    assert_eq!(state.diagnostics().transaction_count, 0);
    assert_eq!(state.diagnostics().promoted_count, 3);
    assert!(state
        .core
        .snapshot()
        .admissions
        .iter()
        .all(|admission| admission.state == AdmissionStateLabel::Released));
}

#[test]
fn revalidating_due_batch_is_recoverable_and_preserved() {
    // Given: a complete due set with one record already owned by revalidation.
    let (mut state, transactions) = due_state(2);
    let before_core = state.core.snapshot();
    let current_tip = VerificationTip::new(Some((Hash([2; 32]), Height(11))));
    assert_eq!(state.begin_revalidation(current_tip, false).len(), 2);
    let mut storage = super::super::storage::Storage::new(&Default::default());

    // When: promotion attempts to snapshot the complete due set.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: every due record remains private and no public or core prefix commits.
    assert_eq!(
        effects.outcome,
        PrivatePromotionOutcome::Recoverable { count: 2 }
    );
    assert!(effects.accepted.is_empty());
    assert!(effects.evicted.is_empty());
    assert_eq!(state.core.snapshot(), before_core);
    assert_eq!(state.diagnostics().transaction_count, 2);
    assert_eq!(state.diagnostics().recoverable_count, 2);
    assert_eq!(storage.transaction_count(), 0);
    assert!(transactions.iter().enumerate().all(|(index, _)| state
        .retained_record(AdmissionId(u64::try_from(index).expect("test index fits")))
        .is_some()));
}

#[test]
fn terminal_public_conflict_removes_only_the_candidate_and_allows_fresh_promotion() {
    // Given: a due private batch whose first candidate already exists publicly.
    let (mut state, transactions) = due_state(2);
    let unaffected_schedule = state.schedule(AdmissionId(1));
    let mut storage = super::super::storage::Storage::new(&Default::default());
    storage
        .insert(transactions[0].clone(), Vec::new(), None)
        .expect("public fixture insertion succeeds");
    let public_before = storage.tx_ids().collect::<HashSet<_>>();

    // When: atomic public preflight rejects the complete private batch.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: only the conflict terminates, while the unaffected candidate keeps its deadline.
    assert_eq!(
        effects.outcome,
        PrivatePromotionOutcome::Terminal { count: 1 }
    );
    assert!(effects.accepted.is_empty());
    assert!(effects.evicted.is_empty());
    assert!(state.retained_record(AdmissionId(0)).is_none());
    assert!(state.retained_record(AdmissionId(1)).is_some());
    assert_eq!(state.schedule(AdmissionId(1)), unaffected_schedule);
    assert_eq!(state.diagnostics().transaction_count, 1);
    assert_eq!(state.diagnostics().terminal_count, 1);
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), public_before);

    // When: a fresh complete preparation runs after selective terminal removal.
    let retry = state.promote_due(&mut storage, &HashSet::new());

    // Then: the remaining candidate promotes without the failed preparation consuming it.
    assert_eq!(
        retry.outcome,
        PrivatePromotionOutcome::Promoted { count: 1 }
    );
    assert_eq!(state.diagnostics().transaction_count, 0);
}

#[test]
fn retained_revalidation_is_marked_once_and_excluded_from_batches() {
    // Given: one retained record verified at the unchanged canonical tip.
    let (mut state, _, context) = retained_state();
    let current_tip = VerificationTip::new(Some((Hash([1; 32]), Height(10))));

    // When: the same-height Reset prepares retained revalidation twice.
    let first = state.begin_revalidation(current_tip, true);
    let second = state.begin_revalidation(current_tip, true);

    // Then: one request owns revalidation and future batch preparation rejects it.
    assert_eq!(first.len(), 1);
    assert!(second.is_empty());
    assert_eq!(
        state.snapshot_batch(&[context.admission_id]),
        Err(super::super::private_pool::PrivatePoolError::RevalidatingAdmission)
    );
}

#[test]
fn mined_grow_terminally_removes_private_record() {
    // Given: one retained record whose mined ID appears in a Grow block.
    let (mut state, verified, context) = retained_state();
    let mined_ids = HashSet::from([verified.transaction.id().mined_id()]);

    // When: mined private records are terminally removed.
    state
        .remove_mined(&mined_ids)
        .expect("core transition and private removal succeed");

    // Then: the pool releases ownership only after the core records removal.
    assert!(state.retained_record(context.admission_id).is_none());
    assert_eq!(state.diagnostics().terminal_count, 1);
    assert_eq!(
        state.core.snapshot().admissions[0].state,
        AdmissionStateLabel::Removed
    );
}
