use thiserror::Error;

/// Result of inserting a verified private record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    /// A new record was retained.
    Inserted,
    /// The exact record was already retained.
    Existing,
}

/// Privacy-safe private-pool operation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PrivatePoolError {
    /// An admission identifier already belongs to another transaction.
    #[error("private admission identity conflicts with an existing record")]
    AdmissionIdConflict,
    /// A transaction is already associated with different private context.
    #[error("private transaction context conflicts with an existing record")]
    TransactionContextConflict,
    /// The candidate depends on an output retained only in the private pool.
    #[error("private transaction depends on a private parent")]
    PrivateParent,
    /// The configured private transaction count is full.
    #[error("private transaction count limit reached")]
    TransactionCountLimit,
    /// The configured private serialized-byte capacity is full.
    #[error("private serialized byte limit reached")]
    SerializedByteLimit,
    /// At least one requested admission is absent.
    #[error("private batch is incomplete")]
    IncompleteBatch,
    /// A batch repeats an admission identifier.
    #[error("private batch contains a duplicate admission")]
    DuplicateBatchAdmission,
    /// A requested admission is undergoing retained contextual verification.
    #[error("private admission is undergoing revalidation")]
    RevalidatingAdmission,
}
