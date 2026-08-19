use crate::{AdmissionId, BatchId, Timestamp};

/// Opaque snapshot of a complete due set awaiting atomic release.
#[derive(Debug, Eq, PartialEq)]
pub struct PreparedRelease {
    pub(crate) observed: Timestamp,
    pub(crate) batch_id: BatchId,
    pub(crate) admission_ids: Vec<AdmissionId>,
}

impl PreparedRelease {
    /// Return the batch identifier that commit will consume.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Borrow admission identifiers ordered by schedule and identifier.
    pub fn admission_ids(&self) -> &[AdmissionId] {
        &self.admission_ids
    }
}
