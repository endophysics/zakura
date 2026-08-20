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

/// Aggregate outcomes from one completed private telemetry window.
#[cfg_attr(feature = "rpc-client", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrivateWindowAggregate {
    /// Number of promoted transactions.
    pub promoted: u64,
    /// Number of recoverable outcomes.
    pub recoverable: u64,
    /// Number of terminal outcomes.
    pub terminal: u64,
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
    /// Outcomes from the most recently completed fixed release-epoch window.
    pub completed_window: Option<PrivateWindowAggregate>,
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
