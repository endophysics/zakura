use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
    transparent::OutPoint,
};
use zakura_node_services::mempool::AdmissionContext;
use zakura_node_services::mempool::AdmissionId;

use super::{
    InsertOutcome, PrivateBatch, PrivatePoolConfig, PrivatePoolError, PrivatePoolStateTotals,
    PrivatePoolStats, PrivateRecord,
};

/// Bounded in-memory owner of verified private records.
pub struct PrivateVerifiedPool {
    config: PrivatePoolConfig,
    records: BTreeMap<AdmissionId, PrivateRecord>,
    admission_by_transaction: HashMap<UnminedTxId, AdmissionId>,
    created_outpoints: HashSet<OutPoint>,
    serialized_bytes: usize,
}

impl PrivateVerifiedPool {
    /// Construct an empty pool with independent private capacity limits.
    pub fn new(config: PrivatePoolConfig) -> Self {
        Self {
            config,
            records: BTreeMap::new(),
            admission_by_transaction: HashMap::new(),
            created_outpoints: HashSet::new(),
            serialized_bytes: 0,
        }
    }

    /// Retain a verified record or report a typed privacy-safe rejection.
    pub fn insert(&mut self, candidate: PrivateRecord) -> Result<InsertOutcome, PrivatePoolError> {
        let admission_id = candidate.admission_id();
        let transaction_id = candidate.transaction_id();

        if let Some(existing) = self.records.get(&admission_id) {
            if existing.transaction_id() != transaction_id {
                return Err(PrivatePoolError::AdmissionIdConflict);
            }
            return if existing == &candidate {
                Ok(InsertOutcome::Existing)
            } else {
                Err(PrivatePoolError::TransactionContextConflict)
            };
        }

        if self.admission_by_transaction.contains_key(&transaction_id) {
            return Err(PrivatePoolError::TransactionContextConflict);
        }
        if candidate
            .spent_mempool_outpoints()
            .iter()
            .any(|outpoint| self.created_outpoints.contains(outpoint))
        {
            return Err(PrivatePoolError::PrivateParent);
        }
        if self.records.len() >= self.config.max_transactions() {
            return Err(PrivatePoolError::TransactionCountLimit);
        }

        let Some(next_bytes) = self
            .serialized_bytes
            .checked_add(candidate.serialized_bytes())
        else {
            return Err(PrivatePoolError::SerializedByteLimit);
        };
        if next_bytes > self.config.max_serialized_bytes() {
            return Err(PrivatePoolError::SerializedByteLimit);
        }

        self.created_outpoints.extend(candidate.created_outpoints());
        self.admission_by_transaction
            .insert(transaction_id, admission_id);
        self.records.insert(admission_id, candidate);
        self.serialized_bytes = next_bytes;
        Ok(InsertOutcome::Inserted)
    }

    #[cfg(test)]
    pub(crate) fn record(&self, admission_id: AdmissionId) -> Option<&PrivateRecord> {
        self.records.get(&admission_id)
    }

    pub(crate) fn classify(
        &self,
        transaction_id: UnminedTxId,
        context: AdmissionContext,
    ) -> PrivatePoolMatch {
        if let Some(existing) = self.records.get(&context.admission_id) {
            return if existing.transaction_id() == transaction_id && existing.context() == context {
                PrivatePoolMatch::Exact
            } else {
                PrivatePoolMatch::AdmissionConflict
            };
        }
        if self.admission_by_transaction.contains_key(&transaction_id) {
            return PrivatePoolMatch::TransactionConflict;
        }
        PrivatePoolMatch::Absent
    }

    pub(crate) fn snapshot_revalidation_requests(
        &self,
        current_tip: super::VerificationTip,
        include_current_tip: bool,
    ) -> Vec<PrivateRecord> {
        self.records
            .values()
            .filter(|record| include_current_tip || record.verification_tip() != current_tip)
            .cloned()
            .collect()
    }

    pub(crate) fn snapshot_mined_records(
        &self,
        mined_ids: &HashSet<transaction::Hash>,
    ) -> Vec<PrivateRecord> {
        self.records
            .values()
            .filter(|record| mined_ids.contains(&record.transaction_id().mined_id()))
            .cloned()
            .collect()
    }

    pub(crate) fn snapshot_expired_records(&self, tip_height: Height) -> Vec<PrivateRecord> {
        self.records
            .values()
            .filter(|record| {
                record
                    .transaction()
                    .transaction
                    .transaction()
                    .expiry_height()
                    .is_some_and(|expiry_height| tip_height >= expiry_height)
            })
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn remove(
        &mut self,
        admission_id: AdmissionId,
    ) -> Result<PrivateRecord, PrivatePoolError> {
        let Some(record) = self.records.remove(&admission_id) else {
            return Err(PrivatePoolError::IncompleteBatch);
        };
        self.admission_by_transaction
            .remove(&record.transaction_id());
        for outpoint in record.created_outpoints() {
            self.created_outpoints.remove(&outpoint);
        }
        self.serialized_bytes -= record.serialized_bytes();
        Ok(record)
    }

    pub(crate) fn replace(&mut self, replacement: PrivateRecord) -> Result<(), PrivatePoolError> {
        let admission_id = replacement.admission_id();
        let Some(existing) = self.records.get_mut(&admission_id) else {
            return Err(PrivatePoolError::IncompleteBatch);
        };
        if existing.transaction_id() != replacement.transaction_id()
            || existing.context() != replacement.context()
        {
            return Err(PrivatePoolError::TransactionContextConflict);
        }

        *existing = replacement;
        Ok(())
    }

    /// Snapshot a complete batch without exposing mutable records.
    pub fn snapshot_batch(
        &self,
        admission_ids: &[AdmissionId],
    ) -> Result<PrivateBatch, PrivatePoolError> {
        let mut unique_ids = HashSet::with_capacity(admission_ids.len());
        let mut records = Vec::with_capacity(admission_ids.len());
        for admission_id in admission_ids {
            if !unique_ids.insert(*admission_id) {
                return Err(PrivatePoolError::DuplicateBatchAdmission);
            }
            let Some(record) = self.records.get(admission_id) else {
                return Err(PrivatePoolError::IncompleteBatch);
            };
            records.push(record.clone());
        }
        Ok(PrivateBatch { records })
    }

    /// Atomically take an exact complete batch in the requested order.
    pub fn remove_batch(
        &mut self,
        admission_ids: &[AdmissionId],
    ) -> Result<PrivateBatch, PrivatePoolError> {
        let batch = self.snapshot_batch(admission_ids)?;
        let removed_ids: HashSet<_> = admission_ids.iter().copied().collect();
        self.remove_admission_ids(&removed_ids);
        Ok(batch)
    }

    pub(crate) fn remove_validated_batch(&mut self, batch: PrivateBatch) {
        let removed_ids = batch.admission_ids().collect::<HashSet<_>>();
        self.remove_admission_ids(&removed_ids);
    }

    fn remove_admission_ids(&mut self, removed_ids: &HashSet<AdmissionId>) {
        self.records
            .retain(|admission_id, _| !removed_ids.contains(admission_id));
        self.admission_by_transaction
            .retain(|_, admission_id| !removed_ids.contains(admission_id));
        self.created_outpoints.clear();
        self.created_outpoints.extend(
            self.records
                .values()
                .flat_map(PrivateRecord::created_outpoints),
        );
        self.serialized_bytes = self
            .records
            .values()
            .map(PrivateRecord::serialized_bytes)
            .sum();
    }

    /// Return aggregate-only capacity and coarse-state diagnostics.
    pub fn stats(&self) -> PrivatePoolStats {
        let transaction_count = self.records.len();
        PrivatePoolStats {
            transaction_count,
            serialized_bytes: self.serialized_bytes,
            max_transactions: self.config.max_transactions(),
            max_serialized_bytes: self.config.max_serialized_bytes(),
            state_totals: PrivatePoolStateTotals {
                verified: transaction_count,
            },
        }
    }
}

pub(crate) enum PrivatePoolMatch {
    Absent,
    Exact,
    AdmissionConflict,
    TransactionConflict,
}

impl fmt::Debug for PrivateVerifiedPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.stats().fmt(formatter)
    }
}
