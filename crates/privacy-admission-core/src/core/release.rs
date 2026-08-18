use super::{AdmissionCore, AdmissionError};
use crate::{AdmissionState, BatchId, Clock, ReleaseOutcome, ReleasedAdmission};

impl<C: Clock> AdmissionCore<C> {
    /// Atomically release the complete due set in deterministic order.
    pub fn release_due(&mut self) -> Result<ReleaseOutcome, AdmissionError> {
        let observed = self.clock.now();
        self.check_clock(observed)?;
        let due = self.records.values().any(|record| match &record.state {
            AdmissionState::Embargoed(state) => state.schedule.scheduled_release_at <= observed,
            AdmissionState::Eligible(_) => true,
            AdmissionState::Released(_)
            | AdmissionState::Rejected(_)
            | AdmissionState::Removed(_) => false,
        });
        if !due {
            self.last_observed = Some(observed);
            return Ok(ReleaseOutcome::NoDue);
        }

        let next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or(AdmissionError::BatchIdExhausted)?;
        let batch_id = BatchId(self.next_batch_id);
        let admission_ids = self.records.keys().copied().collect::<Vec<_>>();
        let mut admissions = Vec::new();
        for (admission_id, record) in admission_ids.into_iter().zip(self.records.values_mut()) {
            let schedule = match &record.state {
                AdmissionState::Embargoed(state)
                    if state.schedule.scheduled_release_at <= observed =>
                {
                    Some(state.schedule)
                }
                AdmissionState::Eligible(state) => Some(state.schedule),
                AdmissionState::Embargoed(_)
                | AdmissionState::Released(_)
                | AdmissionState::Rejected(_)
                | AdmissionState::Removed(_) => None,
            };
            if let Some(schedule) = schedule {
                record.state = AdmissionState::Released(ReleasedAdmission {
                    schedule,
                    terminal_at: observed,
                    batch_id,
                });
                admissions.push(record.view(admission_id));
            }
        }
        admissions.sort_by_key(|view| (view.state.scheduled_release_at(), view.admission_id));
        self.last_observed = Some(observed);
        self.next_batch_id = next_batch_id;
        Ok(ReleaseOutcome::Released {
            batch_id,
            admissions,
        })
    }
}
