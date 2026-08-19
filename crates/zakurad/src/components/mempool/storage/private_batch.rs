use std::collections::HashSet;

use zakura_chain::{transaction::UnminedTxId, transparent};

use crate::components::mempool::{private_pool::PrivateBatch, MempoolError};

use super::{
    AtomicBatchEffects, AtomicBatchInsertError, SameEffectsChainRejectionError, Storage,
    VerifiedSet,
};

struct AtomicBatchPlan {
    verified: VerifiedSet,
    effects: AtomicBatchEffects,
    eviction_victims: Vec<UnminedTxId>,
    pending_outputs: Vec<(transparent::OutPoint, transparent::Output)>,
}

impl AtomicBatchPlan {
    fn commit(self, storage: &mut Storage) -> AtomicBatchEffects {
        let mut previous = std::mem::replace(&mut storage.verified, self.verified.publish());
        previous.silence_metrics();
        drop(previous);

        for victim in self.eviction_victims {
            storage.reject(
                victim,
                SameEffectsChainRejectionError::RandomlyEvicted.into(),
            );
        }
        for (outpoint, output) in self.pending_outputs {
            storage.pending_outputs.respond(&outpoint, output);
        }

        self.effects
    }
}

impl Storage {
    pub fn insert_private_batch(
        &mut self,
        batch: &PrivateBatch,
    ) -> Result<AtomicBatchEffects, AtomicBatchInsertError> {
        for record in batch.records() {
            let transaction = record.transaction();
            let id = transaction.transaction.id();
            if let Some(error) = self.rejection_error(&id) {
                return Err(AtomicBatchInsertError::Candidate {
                    admission_id: record.admission_id(),
                    source: error,
                });
            }
            if self.verified.contains(&id.mined_id()) {
                return Err(AtomicBatchInsertError::Candidate {
                    admission_id: record.admission_id(),
                    source: MempoolError::InMempool,
                });
            }
            self.check_standard_tx(transaction)
                .map_err(MempoolError::from)
                .map_err(|source| AtomicBatchInsertError::Candidate {
                    admission_id: record.admission_id(),
                    source,
                })?;
        }

        let mut verified = self.verified.detached_clone();
        let mut accepted = HashSet::with_capacity(batch.records().len());
        let mut evicted = HashSet::new();
        let mut eviction_victims = Vec::new();
        let mut pending_outputs = Vec::new();

        for record in batch.records() {
            let transaction = record.transaction().clone();
            let id = transaction.transaction.id();
            verified
                .insert(
                    transaction.clone(),
                    record.spent_mempool_outpoints().to_vec(),
                    None,
                    record.verification_tip().height(),
                )
                .map_err(MempoolError::from)
                .map_err(|source| AtomicBatchInsertError::Candidate {
                    admission_id: record.admission_id(),
                    source,
                })?;
            accepted.insert(id);

            while verified.total_cost() > self.tx_cost_limit {
                let removed = verified
                    .evict_one()
                    .ok_or(AtomicBatchInsertError::EvictionInvariant)?;
                let victim = removed
                    .last()
                    .map(|transaction| transaction.transaction.id())
                    .ok_or(AtomicBatchInsertError::EvictionInvariant)?;
                eviction_victims.push(victim);
                evicted.extend(
                    removed
                        .into_iter()
                        .map(|transaction| transaction.transaction.id()),
                );
            }
        }

        let admission_ids = batch
            .records()
            .iter()
            .filter(|record| !verified.contains(&record.transaction_id().mined_id()))
            .map(|record| record.admission_id())
            .collect::<HashSet<_>>();
        if !admission_ids.is_empty() {
            return Err(AtomicBatchInsertError::BatchMemberEvicted { admission_ids });
        }

        for record in batch.records() {
            let transaction = record.transaction();
            let id = transaction.transaction.id().mined_id();
            pending_outputs.extend(
                transaction
                    .transaction
                    .transaction()
                    .outputs()
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, output)| (transparent::OutPoint::from_usize(id, index), output)),
            );
        }

        Ok(AtomicBatchPlan {
            verified,
            effects: AtomicBatchEffects { accepted, evicted },
            eviction_victims,
            pending_outputs,
        }
        .commit(self))
    }
}
