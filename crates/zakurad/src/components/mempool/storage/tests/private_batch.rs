use std::collections::HashSet;

use futures::FutureExt as _;
use zakura_chain::{
    block::{Hash, Height},
    parameters::Network,
    transaction::VerifiedUnminedTx,
    transparent::OutPoint,
};
use zakura_node_services::mempool::{AdmissionContext, AdmissionId, AdmissionPolicy};

use crate::components::mempool::{
    config,
    private_pool::{
        PrivateBatch, PrivateRecord, PrivateRecordFields, PrivateVerifiedPool, VerificationTip,
    },
    storage::{AtomicBatchInsertError, Storage},
    MempoolError, SameEffectsChainRejectionError, SameEffectsTipRejectionError,
};

use super::prop::conflicting_transactions_fixture;

fn batch(transactions: Vec<VerifiedUnminedTx>) -> PrivateBatch {
    let mut pool = PrivateVerifiedPool::new(Default::default());
    let admission_ids: Vec<_> = transactions
        .into_iter()
        .enumerate()
        .map(|(index, transaction)| {
            let admission_id = AdmissionId(
                u64::try_from(index).expect("test batch length fits in an admission ID"),
            );
            pool.insert(PrivateRecord::new(PrivateRecordFields {
                transaction,
                spent_mempool_outpoints: Vec::new(),
                context: AdmissionContext {
                    admission_id,
                    policy: AdmissionPolicy::FixedEpoch,
                },
                verification_tip: VerificationTip::new(Some((Hash([1; 32]), Height(100)))),
            }))
            .expect("test private pool has capacity");
            admission_id
        })
        .collect();

    pool.snapshot_batch(&admission_ids)
        .expect("all inserted test records are present")
}

fn output_transactions(count: usize) -> Vec<VerifiedUnminedTx> {
    Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .filter(|transaction| {
            let outputs = transaction.transaction.transaction().outputs();
            !outputs.is_empty() && outputs.iter().all(|output| !output.is_dust())
        })
        .take(count)
        .collect()
}

#[test]
fn private_batch_commit_returns_accepted_ids_and_wakes_output_waiters() {
    // Given: two private records and a waiter for an output created by the first record.
    let transactions = output_transactions(2);
    assert_eq!(
        transactions.len(),
        2,
        "test vectors contain two output transactions"
    );
    let expected_ids: HashSet<_> = transactions
        .iter()
        .map(|transaction| transaction.transaction.id())
        .collect();
    let first = &transactions[0];
    let first_outpoint = OutPoint::from_usize(first.transaction.id().mined_id(), 0);
    let expected_output = first.transaction.transaction().outputs()[0].clone();
    let batch = batch(transactions);
    let mut storage = Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    let mut output = Box::pin(storage.pending_outputs.queue(first_outpoint));
    assert!(output.as_mut().now_or_never().is_none());

    // When: storage accepts the complete private batch.
    let effects = storage
        .insert_private_batch(&batch)
        .expect("valid private batch commits");

    // Then: all candidates are public together and commit-time effects are returned.
    assert_eq!(effects.accepted, expected_ids);
    assert!(effects.evicted.is_empty());
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), expected_ids);
    let response = output
        .as_mut()
        .now_or_never()
        .expect("commit wakes the pending output request")
        .expect("pending output response succeeds");
    let zakura_node_services::mempool::Response::UnspentOutput(output) = response else {
        panic!("pending output request returned the wrong response variant");
    };
    assert_eq!(output, expected_output);
}

#[test]
fn private_batch_late_failure_leaves_storage_and_output_waiters_unchanged() {
    // Given: an output-producing candidate followed by two candidates that conflict with each other.
    let mut transactions = output_transactions(1);
    assert_eq!(
        transactions.len(),
        1,
        "test vectors contain an output transaction"
    );
    let conflicting = conflicting_transactions_fixture();
    transactions.extend([conflicting.0, conflicting.1]);
    let first_id = transactions[0].transaction.id();
    let first_outpoint = OutPoint::from_usize(first_id.mined_id(), 0);
    let batch = batch(transactions);
    let mut storage = Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    let rejected_before = storage.rejected_transaction_count();
    let mut output = Box::pin(storage.pending_outputs.queue(first_outpoint));

    // When: detached planning reaches the second conflicting candidate.
    let result = storage.insert_private_batch(&batch);

    // Then: neither candidate nor any failed-attempt effect becomes public.
    assert_eq!(
        result,
        Err(AtomicBatchInsertError::Candidate {
            admission_id: AdmissionId(2),
            source: MempoolError::StorageEffectsTip(SameEffectsTipRejectionError::SpendConflict),
        })
    );
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), HashSet::new());
    assert_eq!(storage.rejected_transaction_count(), rejected_before);
    assert!(!storage.contains_rejected(&first_id));
    assert!(output.as_mut().now_or_never().is_none());
}

#[test]
fn private_batch_commit_reports_a_selected_public_eviction_victim() {
    // Given: one public transaction and a complete private batch that together exceed the limit.
    let mut transactions = output_transactions(3).into_iter();
    let public = transactions.next().expect("public test transaction");
    let private: Vec<_> = transactions.collect();
    assert_eq!(private.len(), 2, "test vectors contain a private batch");
    let public_id = public.transaction.id();
    let accepted: HashSet<_> = private
        .iter()
        .map(|transaction| transaction.transaction.id())
        .collect();
    let private_cost: u64 = private.iter().map(VerifiedUnminedTx::cost).sum();
    let batch = batch(private);
    let mut storage = Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    storage
        .insert(public, Vec::new(), None)
        .expect("public transaction is accepted before planning");
    storage.tx_cost_limit = private_cost;
    storage.verified.set_eviction_order([public_id.mined_id()]);

    // When: detached planning selects the public transaction as its ZIP-401 victim.
    let effects = storage
        .insert_private_batch(&batch)
        .expect("the full private batch survives planning");

    // Then: one commit publishes the full batch and reports and rejects the fixed public victim.
    assert_eq!(effects.accepted, accepted);
    assert_eq!(effects.evicted, HashSet::from([public_id]));
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), accepted);
    assert_eq!(
        storage.rejection_error(&public_id),
        Some(SameEffectsChainRejectionError::RandomlyEvicted.into())
    );
}

#[test]
fn selected_private_batch_victim_rolls_back_every_public_effect() {
    // Given: existing public state, a private output waiter, and a fixed private eviction victim.
    let mut transactions = output_transactions(3).into_iter();
    let public = transactions.next().expect("public test transaction");
    let private: Vec<_> = transactions.collect();
    assert_eq!(private.len(), 2, "test vectors contain a private batch");
    let public_id = public.transaction.id();
    let private_ids: Vec<_> = private
        .iter()
        .map(|transaction| transaction.transaction.id())
        .collect();
    let first_private_outpoint = OutPoint::from_usize(private_ids[0].mined_id(), 0);
    let public_cost = public.cost();
    let largest_private_cost = private
        .iter()
        .map(VerifiedUnminedTx::cost)
        .max()
        .expect("private batch is nonempty");
    let batch = batch(private);
    let mut storage = Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        ..Default::default()
    });
    storage
        .insert(public, Vec::new(), None)
        .expect("public transaction is accepted before planning");
    storage.tx_cost_limit = public_cost + largest_private_cost;
    storage
        .verified
        .set_eviction_order([private_ids[0].mined_id()]);
    let rejected_before = storage.rejected_transaction_count();
    let mut output = Box::pin(storage.pending_outputs.queue(first_private_outpoint));

    // When: ZIP-401 planning selects one member of the private batch.
    let result = storage.insert_private_batch(&batch);

    // Then: the complete attempt rolls back without public rejection or output effects.
    assert_eq!(
        result,
        Err(AtomicBatchInsertError::BatchMemberEvicted {
            admission_ids: HashSet::from([AdmissionId(0)]),
        })
    );
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([public_id])
    );
    assert_eq!(storage.rejected_transaction_count(), rejected_before);
    for private_id in private_ids {
        assert!(!storage.contains_rejected(&private_id));
    }
    assert!(output.as_mut().now_or_never().is_none());
}

#[test]
fn private_batch_errors_redact_admission_identities() {
    // Given: adapter errors carrying sentinel private admission identifiers.
    let errors = [
        AtomicBatchInsertError::Candidate {
            admission_id: AdmissionId(1_844),
            source: MempoolError::InMempool,
        },
        AtomicBatchInsertError::BatchMemberEvicted {
            admission_ids: HashSet::from([AdmissionId(7_331)]),
        },
    ];

    // When: the errors cross diagnostic formatting boundaries.
    let formatted = errors
        .iter()
        .flat_map(|error| [format!("{error:?}"), error.to_string()])
        .collect::<Vec<_>>()
        .join(" ");

    // Then: neither opaque identity is disclosed.
    assert!(!formatted.contains("1844"));
    assert!(!formatted.contains("7331"));
}
