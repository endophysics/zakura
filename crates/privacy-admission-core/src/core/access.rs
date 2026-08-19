use super::AdmissionCore;
use crate::{
    AdmissionId, AdmissionState, AdmissionView, Clock, DiagnosticAdmission, DiagnosticSnapshot,
    ReleasePolicy, Timestamp, DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION,
};

impl<C: Clock> AdmissionCore<C> {
    /// Return a shared reference to the configured clock.
    pub const fn clock(&self) -> &C {
        &self.clock
    }

    /// Return a mutable reference to the configured clock.
    pub const fn clock_mut(&mut self) -> &mut C {
        &mut self.clock
    }

    /// Return the configured release policy.
    pub const fn policy(&self) -> &ReleasePolicy {
        &self.policy
    }

    /// Return the number of admission records retained by the core.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Return embargoed admission identifiers in ascending order.
    pub fn embargoed_ids(&self) -> Vec<AdmissionId> {
        self.records
            .iter()
            .filter_map(|(admission_id, record)| match &record.state {
                AdmissionState::Embargoed(_) => Some(*admission_id),
                AdmissionState::Eligible(_)
                | AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            })
            .collect()
    }

    /// Return eligible admission identifiers in ascending order.
    pub fn eligible_ids(&self) -> Vec<AdmissionId> {
        self.records
            .iter()
            .filter_map(|(admission_id, record)| match &record.state {
                AdmissionState::Eligible(_) => Some(*admission_id),
                AdmissionState::Embargoed(_)
                | AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            })
            .collect()
    }

    /// Return the earliest scheduled release among nonterminal admissions.
    pub fn earliest_release_at(&self) -> Option<Timestamp> {
        self.records
            .values()
            .filter_map(|record| match &record.state {
                AdmissionState::Embargoed(state) => Some(state.schedule.scheduled_release_at),
                AdmissionState::Eligible(state) => Some(state.schedule.scheduled_release_at),
                AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            })
            .min()
    }

    /// Return an owned view of one admission.
    pub fn get(&self, admission_id: AdmissionId) -> Option<AdmissionView> {
        self.records
            .get(&admission_id)
            .map(|record| record.view(admission_id))
    }

    /// Return a versioned plaintext-free snapshot ordered by admission identifier.
    pub fn snapshot(&self) -> DiagnosticSnapshot {
        let admissions = self
            .records
            .iter()
            .map(|(admission_id, record)| DiagnosticAdmission {
                admission_id: *admission_id,
                origin: record.origin,
                state: record.state.label(),
                accepted_at_ns: record.state.accepted_at().as_nanos(),
                scheduled_release_at_ns: record.state.scheduled_release_at().as_nanos(),
                terminal_at_ns: record.state.terminal_at().map(Timestamp::as_nanos),
                batch_id: record.state.batch_id(),
                reason: record.state.reason().cloned(),
            })
            .collect();
        DiagnosticSnapshot {
            schema_version: DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION,
            admissions,
        }
    }
}
