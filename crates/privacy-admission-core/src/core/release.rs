use super::{AdmissionCore, AdmissionError};
use crate::{
    AdmissionId, AdmissionState, BatchId, Clock, PreparedRelease, ReleaseOutcome,
    ReleasedAdmission, Timestamp,
};

impl<C: Clock> AdmissionCore<C> {
    /// Snapshot the complete due set without changing admission lifecycle state.
    pub fn prepare_release(&mut self) -> Result<Option<PreparedRelease>, AdmissionError> {
        let observed = self.clock.now();
        self.check_clock(observed)?;
        let admission_ids = self.due_admission_ids(observed);
        if !admission_ids.is_empty() {
            self.next_batch_id
                .checked_add(1)
                .ok_or(AdmissionError::BatchIdExhausted)?;
        }
        self.last_observed = Some(observed);
        if admission_ids.is_empty() {
            return Ok(None);
        }

        Ok(Some(PreparedRelease {
            observed,
            batch_id: BatchId(self.next_batch_id),
            admission_ids,
        }))
    }

    /// Atomically release every admission in a current preparation.
    pub fn commit_release(
        &mut self,
        prepared: PreparedRelease,
    ) -> Result<ReleaseOutcome, AdmissionError> {
        if self.last_observed != Some(prepared.observed)
            || prepared.batch_id != BatchId(self.next_batch_id)
            || self.due_admission_ids(prepared.observed) != prepared.admission_ids
        {
            return Err(AdmissionError::StalePreparedRelease);
        }
        let next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or(AdmissionError::BatchIdExhausted)?;
        let mut admissions = Vec::with_capacity(prepared.admission_ids.len());
        for (admission_id, record) in &mut self.records {
            let schedule = due_schedule(&record.state, prepared.observed);
            if let Some(schedule) = schedule {
                record.state = AdmissionState::Released(ReleasedAdmission {
                    schedule,
                    terminal_at: prepared.observed,
                    batch_id: prepared.batch_id,
                });
                admissions.push(record.view(*admission_id));
            }
        }
        admissions.sort_by_key(|view| (view.state.scheduled_release_at(), view.admission_id));
        self.next_batch_id = next_batch_id;
        Ok(ReleaseOutcome::Released {
            batch_id: prepared.batch_id,
            admissions,
        })
    }

    /// Atomically release the complete due set in deterministic order.
    pub fn release_due(&mut self) -> Result<ReleaseOutcome, AdmissionError> {
        let prior_observation = self.last_observed;
        let Some(prepared) = self.prepare_release()? else {
            return Ok(ReleaseOutcome::NoDue);
        };
        match self.commit_release(prepared) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                self.last_observed = prior_observation;
                Err(error)
            }
        }
    }

    fn due_admission_ids(&self, observed: Timestamp) -> Vec<AdmissionId> {
        let mut admission_ids = self
            .records
            .iter()
            .filter_map(|(admission_id, record)| {
                due_schedule(&record.state, observed)
                    .map(|schedule| (schedule.scheduled_release_at, *admission_id))
            })
            .collect::<Vec<_>>();
        admission_ids.sort_unstable();
        admission_ids
            .into_iter()
            .map(|(_, admission_id)| admission_id)
            .collect()
    }
}

fn due_schedule(state: &AdmissionState, observed: Timestamp) -> Option<crate::AdmissionSchedule> {
    match state {
        AdmissionState::Embargoed(state) if state.schedule.scheduled_release_at <= observed => {
            Some(state.schedule)
        }
        AdmissionState::Eligible(state) => Some(state.schedule),
        AdmissionState::Embargoed(_)
        | AdmissionState::Released(_)
        | AdmissionState::Rejected(_)
        | AdmissionState::Removed(_) => None,
    }
}
