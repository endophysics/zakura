//! Primitives for privacy-preserving admission scheduling.

/// Synchronous admission and release state machine.
pub mod core;
/// Domain identifiers, origins, and rejection reasons.
pub mod id;
mod prepared;
/// Typed persisted states and diagnostic projections.
pub mod state;
/// Clock and release-scheduling primitives.
pub mod time;

pub use core::{AdmissionCore, AdmissionError};
pub use id::{AdmissionId, AdmissionOrigin, BatchId, ReasonCode, ReasonCodeError};
pub use prepared::PreparedRelease;
pub use state::{
    AdmissionOutcome, AdmissionSchedule, AdmissionState, AdmissionStateLabel, AdmissionView,
    DiagnosticAdmission, DiagnosticSnapshot, EligibleAdmission, EmbargoedAdmission,
    RejectedAdmission, ReleaseOutcome, ReleasedAdmission, RemovedAdmission, TransitionOutcome,
    DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION,
};
pub use time::{
    Clock, ManualClock, MonotonicClock, ReleasePolicy, ReleasePolicyError, Timestamp,
    TimestampError,
};
