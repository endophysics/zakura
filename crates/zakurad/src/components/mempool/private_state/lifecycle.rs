use std::collections::HashSet;

use privacy_admission_core::ReasonCode;
use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
};
use zakura_node_services::mempool::{AdmissionContext, AdmissionId};

use super::{PrivateAdmissionState, PrivateTelemetryOutcome};
use crate::components::mempool::{
    downloads::TransactionDownloadVerifyError,
    private_pool::{PrivateBatch, PrivatePoolError, PrivatePoolMatch, PrivateRecord},
    BoxError, VerificationTip,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TerminalKind {
    Reject,
    Remove,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TerminalDecision {
    pub kind: TerminalKind,
    pub reason: &'static str,
}

impl TerminalDecision {
    pub const fn reject(reason: &'static str) -> Self {
        Self {
            kind: TerminalKind::Reject,
            reason,
        }
    }

    pub const fn remove(reason: &'static str) -> Self {
        Self {
            kind: TerminalKind::Remove,
            reason,
        }
    }
}

pub(super) const fn revalidation_terminal_decision(
    error: &TransactionDownloadVerifyError,
) -> Option<TerminalDecision> {
    match error {
        TransactionDownloadVerifyError::InState => Some(TerminalDecision::remove("mined")),
        TransactionDownloadVerifyError::PolicyRejected(_) => {
            Some(TerminalDecision::reject("policy_rejected"))
        }
        TransactionDownloadVerifyError::Invalid { .. } => {
            Some(TerminalDecision::reject("consensus_rejected"))
        }
        TransactionDownloadVerifyError::StateError(_)
        | TransactionDownloadVerifyError::DownloadFailed(_)
        | TransactionDownloadVerifyError::Cancelled => None,
    }
}

impl PrivateAdmissionState {
    pub(in crate::components::mempool) fn begin_revalidation(
        &mut self,
        current_tip: VerificationTip,
        include_current_tip: bool,
    ) -> Vec<PrivateRecord> {
        self.pool
            .snapshot_revalidation_requests(current_tip, include_current_tip)
            .into_iter()
            .filter(|record| self.revalidating.insert(record.admission_id()))
            .collect()
    }

    pub(in crate::components::mempool) fn revalidation_queue_failed(
        &mut self,
        transaction_id: UnminedTxId,
        context: AdmissionContext,
    ) {
        if self.revalidating.contains(&context.admission_id)
            && matches!(
                self.pool.classify(transaction_id, context),
                PrivatePoolMatch::Exact
            )
        {
            self.revalidating.remove(&context.admission_id);
            self.record_telemetry(PrivateTelemetryOutcome::Recoverable, 1);
        }
    }

    pub(crate) fn snapshot_batch(
        &self,
        admission_ids: &[AdmissionId],
    ) -> Result<PrivateBatch, PrivatePoolError> {
        if admission_ids
            .iter()
            .any(|admission_id| self.revalidating.contains(admission_id))
        {
            return Err(PrivatePoolError::RevalidatingAdmission);
        }
        self.pool.snapshot_batch(admission_ids)
    }

    pub(in crate::components::mempool) fn remove_mined(
        &mut self,
        mined_ids: &HashSet<transaction::Hash>,
    ) -> Result<(), BoxError> {
        let decisions = self
            .pool
            .snapshot_mined_records(mined_ids)
            .into_iter()
            .map(|record| (record.admission_id(), TerminalDecision::remove("mined")))
            .collect::<Vec<_>>();
        self.terminalize(&decisions)?;
        Ok(())
    }

    pub(in crate::components::mempool) fn remove_expired(
        &mut self,
        tip_height: Option<Height>,
    ) -> Result<usize, BoxError> {
        let Some(tip_height) = tip_height else {
            return Ok(0);
        };
        let decisions = self
            .pool
            .snapshot_expired_records(tip_height)
            .into_iter()
            .map(|record| (record.admission_id(), TerminalDecision::remove("expired")))
            .collect::<Vec<_>>();
        self.terminalize(&decisions)
    }

    pub(super) fn terminalize(
        &mut self,
        decisions: &[(AdmissionId, TerminalDecision)],
    ) -> Result<usize, BoxError> {
        if decisions.is_empty() {
            return Ok(0);
        }
        let admission_ids = decisions
            .iter()
            .map(|(admission_id, _)| *admission_id)
            .collect::<Vec<_>>();
        let batch = self.pool.snapshot_batch(&admission_ids)?;
        let mut staged_core = self.core.clone();
        for (admission_id, decision) in decisions {
            let reason = ReasonCode::try_from(decision.reason)?;
            match decision.kind {
                TerminalKind::Reject => staged_core.reject(*admission_id, reason)?,
                TerminalKind::Remove => staged_core.remove(*admission_id, reason)?,
            };
        }
        self.core = staged_core;
        self.pool.remove_validated_batch(batch);
        for admission_id in admission_ids {
            self.revalidating.remove(&admission_id);
        }
        self.record_telemetry(PrivateTelemetryOutcome::Terminal, decisions.len());
        self.publish_release_deadline();
        Ok(decisions.len())
    }
}
