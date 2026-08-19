/// Stable private-admission identifier.
pub use privacy_admission_core::AdmissionId;

/// Result of a private admission request without admission identity.
#[cfg_attr(feature = "rpc-client", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "rpc-client", serde(rename_all = "snake_case"))]
pub enum PrivateAdmissionStatus {
    /// A new reservation was accepted and queued for verification.
    Accepted,
    /// The exact retained or in-flight request already exists.
    Existing,
}

/// Scheduling policy selected for private admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionPolicy {
    /// Fixed-epoch release scheduling.
    FixedEpoch,
}

/// Stable identity and scheduling metadata for one private submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionContext {
    /// Stable private-admission identity.
    pub admission_id: AdmissionId,
    /// Selected release scheduling policy.
    pub policy: AdmissionPolicy,
}

/// Aggregate scheduler health.
#[cfg_attr(feature = "rpc-client", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "rpc-client", serde(rename_all = "snake_case"))]
pub enum SchedulerState {
    /// Release scheduling is not implemented or running.
    Idle,
    /// Scheduler is processing release work.
    Running,
    /// Scheduler is stopping.
    Stopping,
    /// Scheduler progress is stale.
    Stalled,
}

/// Aggregate-only private-pool diagnostics.
#[cfg_attr(feature = "rpc-client", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivatePoolDiagnostics {
    /// Number of retained verified transactions.
    pub transaction_count: usize,
    /// Total retained serialized bytes.
    pub serialized_bytes: usize,
    /// Configured retained transaction limit.
    pub max_transactions: usize,
    /// Configured retained serialized-byte limit.
    pub max_serialized_bytes: usize,
    /// Number of embargoed admissions.
    pub embargoed_count: usize,
    /// Number of eligible admissions.
    pub eligible_count: usize,
    /// Number of admissions being released.
    pub releasing_count: usize,
    /// Aggregate scheduler health.
    pub scheduler_state: SchedulerState,
    /// Total promoted transactions.
    pub promoted_count: usize,
    /// Total recoverable promotion outcomes.
    pub recoverable_count: usize,
    /// Total terminal promotion outcomes.
    pub terminal_count: usize,
}

/// Aggregate result of one private promotion attempt.
#[cfg_attr(feature = "rpc-client", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "rpc-client", serde(rename_all = "snake_case"))]
pub enum PrivatePromotionOutcome {
    /// No admission was due.
    NoDue,
    /// A complete batch was promoted.
    Promoted {
        /// Number of promoted transactions.
        count: usize,
    },
    /// Candidates remain retained for retry.
    Recoverable {
        /// Number of retained transactions.
        count: usize,
    },
    /// Candidates reached terminal outcomes.
    Terminal {
        /// Number of terminal transactions.
        count: usize,
    },
}
