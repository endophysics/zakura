use zakura_chain::{
    block::{Hash, Height},
    parameters::Network,
    transaction::VerifiedUnminedTx,
    transparent::OutPoint,
};
use zakura_node_services::mempool::{AdmissionContext, AdmissionId, AdmissionPolicy};

use super::{
    InsertOutcome, PrivatePoolConfig, PrivatePoolError, PrivateRecord, PrivateRecordFields,
    PrivateReleaseConfig, PrivateVerifiedPool, VerificationTip,
};

mod batches;
mod insertion;
mod privacy;

const LARGE_LIMIT: usize = 16 * 1024 * 1024;

fn config(max_transactions: usize, max_serialized_bytes: usize) -> PrivatePoolConfig {
    let release = PrivateReleaseConfig::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(2),
    )
    .expect("test release policy is valid");
    PrivatePoolConfig::new(max_transactions, max_serialized_bytes, release)
        .expect("test limits are nonzero")
}

fn transactions(count: usize) -> Vec<VerifiedUnminedTx> {
    let transactions: Vec<_> = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .take(count)
        .collect();
    assert_eq!(
        transactions.len(),
        count,
        "test vectors contain enough transactions"
    );
    transactions
}

fn transaction_with_output() -> VerifiedUnminedTx {
    Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .find(|transaction| !transaction.transaction.transaction().outputs().is_empty())
        .expect("test vectors contain a transaction with a transparent output")
}

fn record(admission_id: u64, transaction: VerifiedUnminedTx) -> PrivateRecord {
    record_with_spends(admission_id, transaction, Vec::new())
}

fn record_with_spends(
    admission_id: u64,
    transaction: VerifiedUnminedTx,
    spent_mempool_outpoints: Vec<OutPoint>,
) -> PrivateRecord {
    PrivateRecord::new(PrivateRecordFields {
        transaction,
        spent_mempool_outpoints,
        context: AdmissionContext {
            admission_id: AdmissionId(admission_id),
            policy: AdmissionPolicy::FixedEpoch,
        },
        verification_tip: VerificationTip::new(Some((Hash([1; 32]), Height(100)))),
    })
}
