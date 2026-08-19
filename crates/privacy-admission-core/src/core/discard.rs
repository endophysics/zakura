use std::collections::btree_map::Entry;

use super::{AdmissionCore, AdmissionError};
use crate::{AdmissionId, AdmissionStateLabel, AdmissionView, Clock};

impl<C: Clock> AdmissionCore<C> {
    /// Compensate for external payload retention failure while an admission is nonterminal.
    pub fn discard_uncommitted(
        &mut self,
        admission_id: AdmissionId,
    ) -> Result<AdmissionView, AdmissionError> {
        match self.records.entry(admission_id) {
            Entry::Vacant(_) => Err(AdmissionError::UnknownAdmission { admission_id }),
            Entry::Occupied(entry) => match entry.get().state.label() {
                AdmissionStateLabel::Embargoed | AdmissionStateLabel::Eligible => {
                    Ok(entry.remove().view(admission_id))
                }
                state @ (AdmissionStateLabel::Released
                | AdmissionStateLabel::Rejected
                | AdmissionStateLabel::Removed) => Err(AdmissionError::TerminalAdmission {
                    admission_id,
                    state,
                }),
            },
        }
    }
}
