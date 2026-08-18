use serde::{Deserialize, Serialize};

use crate::{AdmissionId, AdmissionOrigin, BatchId, ReasonCode, Timestamp};

/// Stable diagnostic label for an admission state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionStateLabel {
    /// Waiting for its scheduled release time.
    Embargoed,
    /// Ready for a release batch.
    Eligible,
    /// Released in an atomic batch.
    Released,
    /// Rejected by policy.
    Rejected,
    /// Terminal policy-removal result.
    Removed,
}

/// Persisted facts shared by every admission state.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::AdmissionSchedule>();
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionSchedule {
    pub(crate) accepted_at: Timestamp,
    pub(crate) scheduled_release_at: Timestamp,
}

impl AdmissionSchedule {
    pub(crate) const fn new(accepted_at: Timestamp, scheduled_release_at: Timestamp) -> Self {
        Self {
            accepted_at,
            scheduled_release_at,
        }
    }
}

/// Persisted state for an admission waiting for its schedule.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::EmbargoedAdmission>();
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbargoedAdmission {
    pub(crate) schedule: AdmissionSchedule,
}

/// Persisted state for an admission ready to release.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::EligibleAdmission>();
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EligibleAdmission {
    pub(crate) schedule: AdmissionSchedule,
}

/// Persisted state for a released admission.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::ReleasedAdmission>();
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleasedAdmission {
    pub(crate) schedule: AdmissionSchedule,
    pub(crate) terminal_at: Timestamp,
    pub(crate) batch_id: BatchId,
}

/// Persisted state for a rejected admission.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::RejectedAdmission>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RejectedAdmission {
    pub(crate) schedule: AdmissionSchedule,
    pub(crate) terminal_at: Timestamp,
    pub(crate) reason: ReasonCode,
}

/// Persisted state for a removed admission.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::RemovedAdmission>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemovedAdmission {
    pub(crate) schedule: AdmissionSchedule,
    pub(crate) terminal_at: Timestamp,
    pub(crate) reason: ReasonCode,
}

/// Typed persisted lifecycle state for one admission.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::AdmissionState>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionState {
    /// Waiting for its schedule.
    Embargoed(EmbargoedAdmission),
    /// Ready to release.
    Eligible(EligibleAdmission),
    /// Released terminal state.
    Released(ReleasedAdmission),
    /// Rejected terminal state.
    Rejected(RejectedAdmission),
    /// Terminal policy-removal state.
    Removed(RemovedAdmission),
}

impl AdmissionState {
    /// Return the stable state label.
    pub const fn label(&self) -> AdmissionStateLabel {
        match self {
            Self::Embargoed(_) => AdmissionStateLabel::Embargoed,
            Self::Eligible(_) => AdmissionStateLabel::Eligible,
            Self::Released(_) => AdmissionStateLabel::Released,
            Self::Rejected(_) => AdmissionStateLabel::Rejected,
            Self::Removed(_) => AdmissionStateLabel::Removed,
        }
    }

    /// Return the original acceptance time.
    pub const fn accepted_at(&self) -> Timestamp {
        self.schedule().accepted_at
    }

    /// Return the immutable scheduled release time.
    pub const fn scheduled_release_at(&self) -> Timestamp {
        self.schedule().scheduled_release_at
    }

    /// Return the terminal transition time, when terminal.
    pub const fn terminal_at(&self) -> Option<Timestamp> {
        match self {
            Self::Embargoed(_) | Self::Eligible(_) => None,
            Self::Released(state) => Some(state.terminal_at),
            Self::Rejected(state) => Some(state.terminal_at),
            Self::Removed(state) => Some(state.terminal_at),
        }
    }

    /// Return the release batch identifier, when released.
    pub const fn batch_id(&self) -> Option<BatchId> {
        match self {
            Self::Embargoed(_) | Self::Eligible(_) | Self::Rejected(_) | Self::Removed(_) => None,
            Self::Released(state) => Some(state.batch_id),
        }
    }

    /// Return the policy reason, when rejected or removed.
    pub const fn reason(&self) -> Option<&ReasonCode> {
        match self {
            Self::Embargoed(_) | Self::Eligible(_) | Self::Released(_) => None,
            Self::Rejected(state) => Some(&state.reason),
            Self::Removed(state) => Some(&state.reason),
        }
    }

    pub(crate) const fn schedule(&self) -> &AdmissionSchedule {
        match self {
            Self::Embargoed(state) => &state.schedule,
            Self::Eligible(state) => &state.schedule,
            Self::Released(state) => &state.schedule,
            Self::Rejected(state) => &state.schedule,
            Self::Removed(state) => &state.schedule,
        }
    }
}

/// Read-only owned view of one admission.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<privacy_admission_core::AdmissionView>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionView {
    /// Opaque admission identifier.
    pub admission_id: AdmissionId,
    /// Origin fixed by the first admission.
    pub origin: AdmissionOrigin,
    /// Current typed persisted state.
    pub state: AdmissionState,
}

/// Result of admitting an identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    /// A new admission was accepted and persisted.
    Accepted(AdmissionView),
    /// The same-origin admission already existed.
    Existing(AdmissionView),
}

/// Result of a single-record state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionOutcome {
    /// The record changed state.
    Updated(AdmissionView),
    /// The current state absorbed or did not require the transition.
    Existing(AdmissionView),
}

/// Result of releasing the complete due set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseOutcome {
    /// No admission was due, so no batch identifier was consumed.
    NoDue,
    /// Every listed admission was released in one atomic batch.
    Released {
        /// Monotonic release-batch identifier.
        batch_id: BatchId,
        /// Released views ordered by schedule and admission identifier.
        admissions: Vec<AdmissionView>,
    },
}

/// Diagnostic snapshot schema version.
pub const DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION: u16 = 1;

/// Plaintext-free diagnostic projection of one admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticAdmission {
    /// Opaque admission identifier.
    pub admission_id: AdmissionId,
    /// Admission origin.
    pub origin: AdmissionOrigin,
    /// Current state label.
    pub state: AdmissionStateLabel,
    /// Original acceptance timestamp in nanoseconds.
    pub accepted_at_ns: u64,
    /// Scheduled release timestamp in nanoseconds.
    pub scheduled_release_at_ns: u64,
    /// Terminal timestamp in nanoseconds, when terminal.
    pub terminal_at_ns: Option<u64>,
    /// Release batch identifier, when released.
    pub batch_id: Option<BatchId>,
    /// Policy reason, when rejected or removed.
    pub reason: Option<ReasonCode>,
}

/// Versioned, plaintext-free diagnostic snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticSnapshot {
    /// Schema version, currently one.
    pub schema_version: u16,
    /// Admissions ordered by identifier.
    pub admissions: Vec<DiagnosticAdmission>,
}
