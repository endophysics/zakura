use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    AdmissionId, AdmissionOrigin, AdmissionOutcome, AdmissionSchedule, AdmissionState,
    AdmissionStateLabel, AdmissionView, Clock, EligibleAdmission, EmbargoedAdmission, ReasonCode,
    RejectedAdmission, ReleasePolicy, ReleasePolicyError, RemovedAdmission, Timestamp,
    TransitionOutcome,
};

mod access;
mod discard;
mod release;

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdmissionRecord {
    origin: AdmissionOrigin,
    state: AdmissionState,
}

impl AdmissionRecord {
    fn view(&self, admission_id: AdmissionId) -> AdmissionView {
        AdmissionView {
            admission_id,
            origin: self.origin,
            state: self.state.clone(),
        }
    }
}

/// Typed failures that leave admission state unchanged.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AdmissionError {
    /// An existing identifier was retried from a different origin.
    #[error("admission {admission_id:?} belongs to {existing:?}, not {requested:?}")]
    ConflictingOrigin {
        /// Conflicting admission identifier.
        admission_id: AdmissionId,
        /// Origin fixed by the first admission.
        existing: AdmissionOrigin,
        /// Origin supplied by the retry.
        requested: AdmissionOrigin,
    },
    /// The clock moved before the last accepted observation.
    #[error("clock rollback from {last_observed:?} to {observed:?}")]
    ClockRollback {
        /// Rejected clock observation.
        observed: Timestamp,
        /// Last accepted clock observation.
        last_observed: Timestamp,
    },
    /// No admission has the requested identifier.
    #[error("unknown admission {admission_id:?}")]
    UnknownAdmission {
        /// Missing admission identifier.
        admission_id: AdmissionId,
    },
    /// The requested operation cannot discard a terminal admission.
    #[error("admission {admission_id:?} is terminal in state {state:?}")]
    TerminalAdmission {
        /// Terminal admission identifier.
        admission_id: AdmissionId,
        /// Terminal state that prevented compensation.
        state: AdmissionStateLabel,
    },
    /// Release scheduling overflowed.
    #[error(transparent)]
    Schedule(#[from] ReleasePolicyError),
    /// Advancing the release-batch identifier would overflow.
    #[error("release batch identifier exhausted")]
    BatchIdExhausted,
    /// The prepared due set no longer matches current admission state.
    #[error("prepared release is stale")]
    StalePreparedRelease,
}

/// Synchronous owner of deterministic admission and release state.
#[derive(Clone, Debug)]
pub struct AdmissionCore<C: Clock> {
    clock: C,
    policy: ReleasePolicy,
    records: BTreeMap<AdmissionId, AdmissionRecord>,
    last_observed: Option<Timestamp>,
    next_batch_id: u64,
}

impl<C: Clock> AdmissionCore<C> {
    /// Create an empty core using the supplied clock and release policy.
    pub fn new(clock: C, policy: ReleasePolicy) -> Self {
        Self {
            clock,
            policy,
            records: BTreeMap::new(),
            last_observed: None,
            next_batch_id: 0,
        }
    }

    /// Admit a new identifier or return its same-origin existing record.
    pub fn admit(
        &mut self,
        admission_id: AdmissionId,
        origin: AdmissionOrigin,
    ) -> Result<AdmissionOutcome, AdmissionError> {
        if let Some(record) = self.records.get(&admission_id) {
            if record.origin != origin {
                return Err(AdmissionError::ConflictingOrigin {
                    admission_id,
                    existing: record.origin,
                    requested: origin,
                });
            }
            return Ok(AdmissionOutcome::Existing(record.view(admission_id)));
        }

        let observed = self.clock.now();
        self.check_clock(observed)?;
        let scheduled_release_at = self.policy.release_at(observed)?;
        let record = AdmissionRecord {
            origin,
            state: AdmissionState::Embargoed(EmbargoedAdmission {
                schedule: AdmissionSchedule::new(observed, scheduled_release_at),
            }),
        };
        let view = record.view(admission_id);
        self.last_observed = Some(observed);
        self.records.insert(admission_id, record);
        Ok(AdmissionOutcome::Accepted(view))
    }

    /// Atomically move every due embargoed admission to eligible.
    pub fn refresh(&mut self) -> Result<Vec<AdmissionId>, AdmissionError> {
        let observed = self.clock.now();
        self.check_clock(observed)?;
        let transitioned = self
            .records
            .iter()
            .filter_map(|(admission_id, record)| match &record.state {
                AdmissionState::Embargoed(state)
                    if state.schedule.scheduled_release_at <= observed =>
                {
                    Some(*admission_id)
                }
                AdmissionState::Embargoed(_)
                | AdmissionState::Eligible(_)
                | AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            })
            .collect::<Vec<_>>();
        for record in self.records.values_mut() {
            let schedule = match &record.state {
                AdmissionState::Embargoed(state)
                    if state.schedule.scheduled_release_at <= observed =>
                {
                    Some(state.schedule)
                }
                AdmissionState::Embargoed(_)
                | AdmissionState::Eligible(_)
                | AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            };
            if let Some(schedule) = schedule {
                record.state = AdmissionState::Eligible(EligibleAdmission { schedule });
            }
        }
        self.last_observed = Some(observed);
        Ok(transitioned)
    }

    /// Reject a nonterminal admission with a machine-readable reason.
    pub fn reject(
        &mut self,
        admission_id: AdmissionId,
        reason: ReasonCode,
    ) -> Result<TransitionOutcome, AdmissionError> {
        self.terminate(admission_id, reason, TerminalKind::Rejected)
    }

    /// Apply a terminal policy-removal decision with a machine-readable reason.
    pub fn remove(
        &mut self,
        admission_id: AdmissionId,
        reason: ReasonCode,
    ) -> Result<TransitionOutcome, AdmissionError> {
        self.terminate(admission_id, reason, TerminalKind::Removed)
    }

    fn terminate(
        &mut self,
        admission_id: AdmissionId,
        reason: ReasonCode,
        kind: TerminalKind,
    ) -> Result<TransitionOutcome, AdmissionError> {
        let record = self
            .records
            .get(&admission_id)
            .ok_or(AdmissionError::UnknownAdmission { admission_id })?;
        let schedule = match &record.state {
            AdmissionState::Embargoed(state) => state.schedule,
            AdmissionState::Eligible(state) => state.schedule,
            AdmissionState::Released(_)
            | AdmissionState::Rejected(_)
            | AdmissionState::Removed(_) => {
                return Ok(TransitionOutcome::Existing(record.view(admission_id)));
            }
        };
        let observed = self.clock.now();
        self.check_clock(observed)?;
        let state = match kind {
            TerminalKind::Rejected => AdmissionState::Rejected(RejectedAdmission {
                schedule,
                terminal_at: observed,
                reason,
            }),
            TerminalKind::Removed => AdmissionState::Removed(RemovedAdmission {
                schedule,
                terminal_at: observed,
                reason,
            }),
        };
        let record = self
            .records
            .get_mut(&admission_id)
            .ok_or(AdmissionError::UnknownAdmission { admission_id })?;
        record.state = state;
        self.last_observed = Some(observed);
        Ok(TransitionOutcome::Updated(record.view(admission_id)))
    }

    pub(super) fn check_clock(&self, observed: Timestamp) -> Result<(), AdmissionError> {
        if let Some(last_observed) = self.last_observed {
            if observed < last_observed {
                return Err(AdmissionError::ClockRollback {
                    observed,
                    last_observed,
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalKind {
    Rejected,
    Removed,
}

#[cfg(test)]
mod tests;
