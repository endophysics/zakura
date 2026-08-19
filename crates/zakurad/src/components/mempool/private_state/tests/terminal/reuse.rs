use std::collections::HashSet;

use privacy_admission_core::AdmissionStateLabel;
use zakura_node_services::mempool::{AdmissionContext, AdmissionId, AdmissionPolicy};

use super::TerminalDecision;
use super::{due_state, retained_state};
use crate::components::mempool::{config, MempoolError};

#[test]
fn terminal_core_known_ids_reject_reuse_without_mutation() {
    // Given: one admission in each terminal core state and no retained record.
    let (mut rejected, rejected_tx, rejected_context) = retained_state();
    rejected
        .terminalize(&[(
            rejected_context.admission_id,
            TerminalDecision::reject("policy_rejected"),
        )])
        .expect("rejection staging succeeds");

    let (mut removed, removed_tx, removed_context) = retained_state();
    removed
        .terminalize(&[(
            removed_context.admission_id,
            TerminalDecision::remove("expired"),
        )])
        .expect("removal staging succeeds");

    let (mut released, released_transactions) = due_state(1);
    let released_tx = released_transactions[0].clone();
    let released_context = AdmissionContext {
        admission_id: AdmissionId(0),
        policy: AdmissionPolicy::FixedEpoch,
    };
    let mut storage = crate::components::mempool::storage::Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    assert!(matches!(
        released.promote_due(&mut storage, &HashSet::new()).outcome,
        zakura_node_services::mempool::PrivatePromotionOutcome::Promoted { count: 1 }
    ));

    // When: each terminal admission ID is reused with its original request.
    for (state, transaction, context, expected_state) in [
        (
            &mut rejected,
            rejected_tx,
            rejected_context,
            AdmissionStateLabel::Rejected,
        ),
        (
            &mut removed,
            removed_tx,
            removed_context,
            AdmissionStateLabel::Removed,
        ),
        (
            &mut released,
            released_tx,
            released_context,
            AdmissionStateLabel::Released,
        ),
    ] {
        let before_core = state.core.snapshot();
        let before_diagnostics = state.diagnostics();
        let result = state.reserve(transaction.transaction, context);

        // Then: terminal diagnostics/state and ownership remain unchanged.
        assert!(matches!(
            result,
            Err(MempoolError::TerminalPrivateAdmission)
        ));
        assert_eq!(state.core.snapshot(), before_core);
        assert_eq!(state.diagnostics(), before_diagnostics);
        assert_eq!(state.reservations.len(), 0);
        assert!(state.retained_record(context.admission_id).is_none());
        assert_eq!(state.core.snapshot().admissions[0].state, expected_state);
    }
}
