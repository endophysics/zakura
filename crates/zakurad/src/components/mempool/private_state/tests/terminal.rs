use std::collections::HashSet;

use privacy_admission_core::AdmissionStateLabel;
use zakura_chain::block::{Hash, Height};
use zakura_consensus::error::TransactionError;
use zakura_node_services::mempool::AdmissionId;
use zakura_state::CloneError;

use super::{due_state, due_state_with_expiry, retained_state};
use crate::components::mempool::{
    config,
    private_state::{
        lifecycle::{revalidation_terminal_decision, TerminalDecision},
        promotion::candidate_terminal_decision,
    },
    storage::{
        ExactTipRejectionError, NonStandardTransactionError, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError,
    },
    MempoolError, TransactionDownloadVerifyError, VerificationTip,
};

mod capacity;
mod reuse;
fn clone_error() -> CloneError {
    CloneError::from(Box::<dyn std::error::Error + Send + Sync>::from(
        std::io::Error::other("test error"),
    ))
}

#[test]
fn promotion_candidate_errors_map_exhaustively() {
    // Given: every deterministic candidate failure and its terminal decision.
    let terminal = [
        (
            MempoolError::StorageExactTip(ExactTipRejectionError::FailedVerification(
                TransactionError::BadBalance,
            )),
            TerminalDecision::reject("consensus_rejected"),
        ),
        (
            MempoolError::StorageExactTip(ExactTipRejectionError::FailedStandard(
                NonStandardTransactionError::IsDust,
            )),
            TerminalDecision::reject("policy_rejected"),
        ),
        (
            MempoolError::NonStandardTransaction(NonStandardTransactionError::IsDust),
            TerminalDecision::reject("policy_rejected"),
        ),
        (
            MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::SpendConflict),
            TerminalDecision::remove("public_conflict"),
        ),
        (
            MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::MissingOutput),
            TerminalDecision::remove("public_conflict"),
        ),
        (
            MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::DuplicateSpend),
            TerminalDecision::remove("public_conflict"),
        ),
        (
            MempoolError::InMempool,
            TerminalDecision::remove("public_conflict"),
        ),
        (
            MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Expired),
            TerminalDecision::remove("expired"),
        ),
        (
            MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Mined),
            TerminalDecision::remove("mined"),
        ),
    ];
    let recoverable = [
        MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::RandomlyEvicted),
        MempoolError::AlreadyQueued,
        MempoolError::ConflictingPrivateAdmission,
        MempoolError::PrivateAdmissionIdConflict,
        MempoolError::TerminalPrivateAdmission,
        MempoolError::PrivatePoolFull,
        MempoolError::PrivateOperationsClosed,
        MempoolError::FullQueue,
        MempoolError::Disabled,
    ];

    // When / Then: deterministic failures terminalize and queue or cache failures recover.
    for (error, expected) in terminal {
        assert_eq!(candidate_terminal_decision(&error), Some(expected));
    }
    for error in recoverable {
        assert_eq!(candidate_terminal_decision(&error), None);
    }
}

#[test]
fn retained_revalidation_errors_map_exhaustively() {
    // Given: every typed retained revalidation result.
    let terminal = [
        (
            TransactionDownloadVerifyError::InState,
            TerminalDecision::remove("mined"),
        ),
        (
            TransactionDownloadVerifyError::PolicyRejected(NonStandardTransactionError::IsDust),
            TerminalDecision::reject("policy_rejected"),
        ),
        (
            TransactionDownloadVerifyError::Invalid {
                error: TransactionError::BadBalance,
                advertiser_addr: None,
            },
            TerminalDecision::reject("consensus_rejected"),
        ),
    ];
    let recoverable = [
        TransactionDownloadVerifyError::StateError(clone_error()),
        TransactionDownloadVerifyError::DownloadFailed(clone_error()),
        TransactionDownloadVerifyError::Cancelled,
    ];

    // When / Then: only deterministic typed results carry terminal decisions.
    for (error, expected) in terminal {
        assert_eq!(revalidation_terminal_decision(&error), Some(expected));
    }
    for error in recoverable {
        assert_eq!(revalidation_terminal_decision(&error), None);
    }
}

#[test]
fn deterministic_retained_failures_terminalize_the_record() {
    // Given: each deterministic retained failure and its core result.
    let cases = [
        (
            TransactionDownloadVerifyError::PolicyRejected(NonStandardTransactionError::IsDust),
            AdmissionStateLabel::Rejected,
            "policy_rejected",
        ),
        (
            TransactionDownloadVerifyError::Invalid {
                error: TransactionError::BadBalance,
                advertiser_addr: None,
            },
            AdmissionStateLabel::Rejected,
            "consensus_rejected",
        ),
        (
            TransactionDownloadVerifyError::InState,
            AdmissionStateLabel::Removed,
            "mined",
        ),
    ];

    // When / Then: each typed result stages core terminal state before private removal.
    for (error, expected_state, expected_reason) in cases {
        let (mut state, verified, context) = retained_state();
        state.begin_revalidation(
            VerificationTip::new(Some((Hash([2; 32]), Height(11)))),
            false,
        );
        state.fail(verified.transaction.id(), context, error);
        assert!(state.retained_record(context.admission_id).is_none());
        let admission = &state.core.snapshot().admissions[0];
        assert_eq!(admission.state, expected_state);
        assert_eq!(
            admission.reason.as_ref().map(|reason| reason.as_str()),
            Some(expected_reason)
        );
    }
}

#[test]
fn expiry_is_inclusive_and_selectively_terminal() {
    // Given: two retained records, only one expiring at height 10.
    let (mut state, _) = due_state_with_expiry(2, Some(Height(10)));

    // When: expiry is checked immediately below and then exactly at the boundary.
    state
        .remove_expired(Some(Height(9)))
        .expect("expiry staging succeeds");
    let removed = state
        .remove_expired(Some(Height(10)))
        .expect("expiry staging succeeds");

    // Then: the exact boundary removes only the expired record.
    assert_eq!(removed, 1);
    assert!(state.retained_record(AdmissionId(0)).is_none());
    assert!(state.retained_record(AdmissionId(1)).is_some());
}

#[test]
fn detached_private_eviction_removes_only_the_victim() {
    // Given: a two-record due batch whose fixed ZIP-401 victim is its first record.
    let (mut state, transactions) = due_state(2);
    let first_cost = transactions[0].cost();
    let second_cost = transactions[1].cost();
    let mut storage = crate::components::mempool::storage::Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    storage.configure_private_promotion_eviction(
        first_cost.max(second_cost),
        [transactions[0].transaction.id().mined_id()],
    );

    // When: detached planning evicts that private batch member.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: only the victim is removed and the unaffected record remains freshly eligible.
    assert_eq!(
        effects.outcome,
        zakura_node_services::mempool::PrivatePromotionOutcome::Terminal { count: 1 }
    );
    assert!(effects.accepted.is_empty());
    assert!(effects.evicted.is_empty());
    assert!(state.retained_record(AdmissionId(0)).is_none());
    assert!(state.retained_record(AdmissionId(1)).is_some());
    let first = &state.core.snapshot().admissions[0];
    assert_eq!(first.state, AdmissionStateLabel::Removed);
    assert_eq!(
        first.reason.as_ref().map(|reason| reason.as_str()),
        Some("private_evicted")
    );
    assert_eq!(storage.transaction_count(), 0);
}

#[test]
fn multi_record_terminal_staging_is_all_or_nothing() {
    // Given: two retained records and a terminal set containing one unknown admission.
    let (mut state, _) = due_state(2);
    let core_before = state.core.snapshot();
    let decisions = [
        (AdmissionId(0), TerminalDecision::remove("expired")),
        (AdmissionId(99), TerminalDecision::remove("expired")),
    ];

    // When: validated-subset staging fails before all records can transition.
    let result = state.terminalize(&decisions);

    // Then: neither core nor private ownership publishes a prefix.
    assert!(result.is_err());
    assert_eq!(state.core.snapshot(), core_before);
    assert!(state.retained_record(AdmissionId(0)).is_some());
    assert!(state.retained_record(AdmissionId(1)).is_some());
}
