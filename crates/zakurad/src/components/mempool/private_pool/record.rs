use std::fmt;

use zakura_chain::{
    transaction::{UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_node_services::mempool::{AdmissionContext, AdmissionId};

use crate::components::mempool::VerificationTip;

/// Typed fields required to retain and later revalidate a private transaction.
#[derive(Clone, PartialEq)]
pub struct PrivateRecordFields {
    /// Fully verified transaction and policy metadata.
    pub transaction: VerifiedUnminedTx,
    /// Public-mempool outpoints used during contextual verification.
    pub spent_mempool_outpoints: Vec<OutPoint>,
    /// Stable private admission identity and scheduling policy.
    pub context: AdmissionContext,
    /// Tip metadata used during contextual verification.
    pub verification_tip: VerificationTip,
}

impl fmt::Debug for PrivateRecordFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrivateRecordFields(private)")
    }
}

/// One verified private transaction and its promotion inputs.
#[derive(Clone, PartialEq)]
pub struct PrivateRecord {
    fields: PrivateRecordFields,
    serialized_bytes: usize,
}

impl PrivateRecord {
    /// Construct a record with serialized-byte accounting derived from the transaction.
    pub fn new(fields: PrivateRecordFields) -> Self {
        let serialized_bytes = fields.transaction.transaction.size();
        Self {
            fields,
            serialized_bytes,
        }
    }

    /// Return the stable admission identifier.
    pub const fn admission_id(&self) -> AdmissionId {
        self.fields.context.admission_id
    }

    /// Return the exact unmined transaction identifier.
    pub fn transaction_id(&self) -> UnminedTxId {
        self.fields.transaction.transaction.id()
    }

    /// Borrow the verified transaction for contextual revalidation or promotion.
    pub const fn transaction(&self) -> &VerifiedUnminedTx {
        &self.fields.transaction
    }

    /// Borrow public-mempool dependencies observed during verification.
    pub fn spent_mempool_outpoints(&self) -> &[OutPoint] {
        &self.fields.spent_mempool_outpoints
    }

    /// Return the private admission context.
    pub const fn context(&self) -> AdmissionContext {
        self.fields.context
    }

    /// Return the contextual verification tip metadata.
    pub const fn verification_tip(&self) -> VerificationTip {
        self.fields.verification_tip
    }

    /// Return the transaction's serialized byte count.
    pub const fn serialized_bytes(&self) -> usize {
        self.serialized_bytes
    }

    pub(super) fn created_outpoints(&self) -> impl Iterator<Item = OutPoint> + '_ {
        let hash = self.transaction_id().mined_id();
        (0u32..)
            .zip(self.transaction().transaction.transaction().outputs())
            .map(move |(index, _)| OutPoint { hash, index })
    }
}

impl fmt::Debug for PrivateRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrivateRecord(private)")
    }
}

/// Immutable ordered snapshot of an exact private batch.
#[derive(Clone, PartialEq)]
pub struct PrivateBatch {
    pub(super) records: Vec<PrivateRecord>,
}

impl PrivateBatch {
    /// Borrow records in the requested admission order.
    pub fn records(&self) -> &[PrivateRecord] {
        &self.records
    }

    /// Iterate over admission identifiers in the requested order.
    pub fn admission_ids(&self) -> impl ExactSizeIterator<Item = AdmissionId> + '_ {
        self.records.iter().map(PrivateRecord::admission_id)
    }
}

impl fmt::Debug for PrivateBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateBatch")
            .field("record_count", &self.records.len())
            .finish()
    }
}
