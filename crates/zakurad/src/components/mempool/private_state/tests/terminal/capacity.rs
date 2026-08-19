use std::collections::HashSet;

use zakura_chain::parameters::Network;
use zakura_node_services::mempool::{AdmissionContext, AdmissionId, AdmissionPolicy};

use super::super::due_state;
use super::TerminalDecision;
use crate::components::mempool::{config, MempoolError};

#[test]
fn terminal_records_consume_total_admission_capacity_without_mutation() {
    // Given: a two-record private state whose records are terminalized and released.
    let (mut state, transactions) = due_state(2);
    state
        .terminalize(&[(AdmissionId(0), TerminalDecision::remove("expired"))])
        .expect("terminal staging succeeds");
    let mut storage = crate::components::mempool::storage::Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    assert!(matches!(
        state.promote_due(&mut storage, &HashSet::new()).outcome,
        zakura_node_services::mempool::PrivatePromotionOutcome::Promoted { count: 1 }
    ));
    assert_eq!(state.core.record_count(), 2);
    assert_eq!(state.diagnostics().transaction_count, 0);
    let before_core = state.core.snapshot();
    let before_diagnostics = state.diagnostics();
    let fresh_transaction = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .nth(2)
        .expect("test vectors contain a fresh transaction");
    assert!(transactions
        .iter()
        .all(|transaction| transaction.transaction.id() != fresh_transaction.transaction.id()));

    // When: a fresh admission ID attempts to reserve another transaction.
    let result = state.reserve(
        fresh_transaction.transaction,
        AdmissionContext {
            admission_id: AdmissionId(2),
            policy: AdmissionPolicy::FixedEpoch,
        },
    );

    // Then: capacity rejects it before reservation or any state mutation.
    assert!(matches!(result, Err(MempoolError::PrivatePoolFull)));
    assert_eq!(state.core.snapshot(), before_core);
    assert_eq!(state.diagnostics(), before_diagnostics);
    assert!(state.reservations.is_empty());
}
