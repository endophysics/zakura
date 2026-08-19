use std::collections::HashSet;

use privacy_admission_core::AdmissionStateLabel;
use zakura_node_services::mempool::{AdmissionId, PrivatePromotionOutcome};

use super::super::super::storage::SameEffectsChainRejectionError;
use super::due_state;

#[test]
fn recoverable_public_failure_preserves_core_until_retry_commits() {
    // Given: a due private batch and a public adapter rejection for one candidate.
    let (mut state, transactions) = due_state(2);
    let before_core = state.core.snapshot();
    let before_deadlines = (0..2)
        .map(|index| state.schedule(AdmissionId(index)))
        .collect::<Vec<_>>();
    let mut storage = super::super::super::storage::Storage::new(&Default::default());
    storage.reject(
        transactions[0].transaction.id(),
        SameEffectsChainRejectionError::RandomlyEvicted.into(),
    );
    let public_before = storage.tx_ids().collect::<HashSet<_>>();

    // When: public insertion rejects the complete batch during promotion.
    let effects = state.promote_due(&mut storage, &HashSet::new());

    // Then: no authoritative release state, deadline, private ownership, or public state publishes.
    assert_eq!(
        effects.outcome,
        PrivatePromotionOutcome::Recoverable { count: 2 }
    );
    assert_eq!(state.core.snapshot(), before_core);
    assert!(state
        .core
        .snapshot()
        .admissions
        .iter()
        .all(|admission| admission.state != AdmissionStateLabel::Released));
    assert_eq!(
        (0..2)
            .map(|index| state.schedule(AdmissionId(index)))
            .collect::<Vec<_>>(),
        before_deadlines
    );
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), public_before);

    // When: a fresh attempt runs after the transient public rejection is cleared.
    storage.clear();
    let retry = state.promote_due(&mut storage, &HashSet::new());

    // Then: release state publishes only with the successful public batch commit.
    assert_eq!(
        retry.outcome,
        PrivatePromotionOutcome::Promoted { count: 2 }
    );
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        transactions
            .iter()
            .map(|transaction| transaction.transaction.id())
            .collect::<HashSet<_>>()
    );
    assert!(state
        .core
        .snapshot()
        .admissions
        .iter()
        .all(|admission| admission.state == AdmissionStateLabel::Released));
}
