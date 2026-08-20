use std::collections::{BTreeMap, HashMap, HashSet};

use privacy_admission_core::{
    AdmissionCore, AdmissionOrigin as CoreOrigin, AdmissionOutcome, AdmissionStateLabel,
    MonotonicClock, Timestamp,
};
use tokio::sync::{oneshot, watch};
use zakura_chain::{
    transaction::{UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_node_services::mempool::{
    AdmissionContext, AdmissionId, PrivatePoolDiagnostics, SchedulerState,
};

use super::{
    private_pool::{
        InsertOutcome, PrivatePoolConfig, PrivatePoolMatch, PrivateRecord, PrivateRecordFields,
        PrivateVerifiedPool, VerificationTip,
    },
    private_release_scheduler::PrivateReleaseTiming,
    BoxError, MempoolError,
};

mod lifecycle;
mod promotion;

struct Reservation {
    transaction: UnminedTx,
    context: AdmissionContext,
    serialized_bytes: usize,
    completion: oneshot::Sender<Result<(), BoxError>>,
}

pub(super) struct PrivateAdmissionState {
    core: AdmissionCore<MonotonicClock>,
    pool: PrivateVerifiedPool,
    reservations: BTreeMap<AdmissionId, Reservation>,
    revalidating: HashSet<AdmissionId>,
    admission_by_transaction: HashMap<UnminedTxId, AdmissionId>,
    reservation_bytes: usize,
    config: PrivatePoolConfig,
    release_deadlines: watch::Sender<Option<Timestamp>>,
    scheduler_state: watch::Receiver<SchedulerState>,
    promoted_count: usize,
    recoverable_count: usize,
    terminal_count: usize,
}

impl PrivateAdmissionState {
    pub(super) fn new(config: PrivatePoolConfig) -> Self {
        let (release_deadlines, _) = watch::channel(None);
        Self {
            core: AdmissionCore::new(MonotonicClock::new(), config.release_policy()),
            pool: PrivateVerifiedPool::new(config),
            reservations: BTreeMap::new(),
            revalidating: HashSet::new(),
            admission_by_transaction: HashMap::new(),
            reservation_bytes: 0,
            config,
            release_deadlines,
            scheduler_state: watch::channel(SchedulerState::Idle).1,
            promoted_count: 0,
            recoverable_count: 0,
            terminal_count: 0,
        }
    }

    pub(super) fn reserve(
        &mut self,
        transaction: UnminedTx,
        context: AdmissionContext,
    ) -> Result<PrivateReservationOutcome, MempoolError> {
        let transaction_id = transaction.id();
        match self.pool.classify(transaction_id, context) {
            PrivatePoolMatch::Exact => return Ok(PrivateReservationOutcome::Existing),
            PrivatePoolMatch::AdmissionConflict => {
                return Err(MempoolError::PrivateAdmissionIdConflict)
            }
            PrivatePoolMatch::TransactionConflict => {
                return Ok(PrivateReservationOutcome::Existing)
            }
            PrivatePoolMatch::Absent => {}
        }

        if let Some(existing) = self.reservations.get(&context.admission_id) {
            return if existing.transaction.id() == transaction_id && existing.context == context {
                Ok(PrivateReservationOutcome::Existing)
            } else {
                Err(MempoolError::PrivateAdmissionIdConflict)
            };
        }
        if self.admission_by_transaction.contains_key(&transaction_id) {
            return Ok(PrivateReservationOutcome::Existing);
        }
        if self
            .core
            .get(context.admission_id)
            .is_some_and(|admission| {
                matches!(
                    admission.state.label(),
                    AdmissionStateLabel::Released
                        | AdmissionStateLabel::Rejected
                        | AdmissionStateLabel::Removed
                )
            })
        {
            return Err(MempoolError::TerminalPrivateAdmission);
        }

        let serialized_bytes = transaction.size();
        let pool_stats = self.pool.stats();
        let combined_count = self
            .core
            .record_count()
            .checked_add(self.reservations.len())
            .and_then(|count| count.checked_add(1));
        let combined_bytes = pool_stats
            .serialized_bytes
            .checked_add(self.reservation_bytes)
            .and_then(|bytes| bytes.checked_add(serialized_bytes));
        if combined_count.is_none_or(|count| count > self.config.max_transactions()) {
            return Err(MempoolError::PrivatePoolFull);
        }
        if combined_bytes.is_none_or(|bytes| bytes > self.config.max_serialized_bytes()) {
            return Err(MempoolError::PrivatePoolFull);
        }

        let (completion, receiver) = oneshot::channel();
        self.reservation_bytes += serialized_bytes;
        self.admission_by_transaction
            .insert(transaction_id, context.admission_id);
        self.reservations.insert(
            context.admission_id,
            Reservation {
                transaction,
                context,
                serialized_bytes,
                completion,
            },
        );
        Ok(PrivateReservationOutcome::Accepted(receiver))
    }

    pub(super) fn complete_verified(
        &mut self,
        transaction: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<OutPoint>,
        verification_tip: VerificationTip,
        context: AdmissionContext,
    ) {
        let transaction_id = transaction.transaction.id();
        if self.revalidating.contains(&context.admission_id) {
            let replacement = PrivateRecord::new(PrivateRecordFields {
                transaction,
                spent_mempool_outpoints,
                context,
                verification_tip,
            });
            let result = self.pool.replace(replacement);
            self.revalidating.remove(&context.admission_id);
            if result.is_err() {
                self.recoverable_count += 1;
            }
            return;
        }

        let Some(reservation) = self.remove_reservation(context.admission_id) else {
            return;
        };
        if reservation.context != context || reservation.transaction.id() != transaction_id {
            let _ = reservation
                .completion
                .send(Err(MempoolError::ConflictingPrivateAdmission.into()));
            return;
        }

        let admission = self
            .core
            .admit(context.admission_id, CoreOrigin::PrivateGateway);
        let newly_admitted = matches!(admission, Ok(AdmissionOutcome::Accepted(_)));
        let result = admission
            .map_err(|_| ())
            .and_then(|_| {
                self.pool
                    .insert(PrivateRecord::new(PrivateRecordFields {
                        transaction,
                        spent_mempool_outpoints,
                        context,
                        verification_tip,
                    }))
                    .map_err(|_| ())
            })
            .map(|outcome| match outcome {
                InsertOutcome::Inserted | InsertOutcome::Existing => (),
            });
        if result.is_err() && newly_admitted {
            let _ = self.core.discard_uncommitted(context.admission_id);
        }
        self.publish_release_deadline();
        let completion = result.map_err(|()| "private transaction retention failed".into());
        let _ = reservation.completion.send(completion);
    }

    #[cfg(test)]
    pub(super) fn retained_record(&self, admission_id: AdmissionId) -> Option<&PrivateRecord> {
        self.pool.record(admission_id)
    }

    pub(super) fn fail(
        &mut self,
        transaction_id: UnminedTxId,
        context: AdmissionContext,
        error: super::TransactionDownloadVerifyError,
    ) {
        if self.revalidating.contains(&context.admission_id)
            && matches!(
                self.pool.classify(transaction_id, context),
                PrivatePoolMatch::Exact
            )
        {
            if let Some(decision) = lifecycle::revalidation_terminal_decision(&error) {
                if self
                    .terminalize(&[(context.admission_id, decision)])
                    .is_ok()
                {
                    return;
                }
            }
            self.revalidating.remove(&context.admission_id);
            self.recoverable_count += 1;
            return;
        }

        self.fail_reservation(transaction_id, context);
    }

    pub(super) fn revalidation_timed_out(
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
            self.recoverable_count += 1;
            return;
        }
        self.fail_reservation(transaction_id, context);
    }

    fn fail_reservation(&mut self, transaction_id: UnminedTxId, context: AdmissionContext) {
        let matches = self
            .reservations
            .get(&context.admission_id)
            .is_some_and(|reservation| {
                reservation.context == context && reservation.transaction.id() == transaction_id
            });
        if matches {
            if let Some(reservation) = self.remove_reservation(context.admission_id) {
                let _ = reservation
                    .completion
                    .send(Err("private transaction verification failed".into()));
            }
        }
    }

    pub(super) fn cancel_reservation(&mut self, context: AdmissionContext) {
        if let Some(transaction_id) = self
            .reservations
            .get(&context.admission_id)
            .map(|reservation| reservation.transaction.id())
        {
            self.fail_reservation(transaction_id, context);
        }
    }

    pub(super) fn diagnostics(&self) -> PrivatePoolDiagnostics {
        let stats = self.pool.stats();
        let snapshot = self.core.snapshot();
        PrivatePoolDiagnostics {
            transaction_count: stats.transaction_count,
            serialized_bytes: stats.serialized_bytes,
            max_transactions: stats.max_transactions,
            max_serialized_bytes: stats.max_serialized_bytes,
            embargoed_count: snapshot
                .admissions
                .iter()
                .filter(|entry| entry.state == AdmissionStateLabel::Embargoed)
                .count(),
            eligible_count: snapshot
                .admissions
                .iter()
                .filter(|entry| {
                    entry.state == AdmissionStateLabel::Eligible
                        && self.snapshot_batch(&[entry.admission_id]).is_ok()
                })
                .count(),
            releasing_count: 0,
            scheduler_state: *self.scheduler_state.borrow(),
            promoted_count: self.promoted_count,
            recoverable_count: self.recoverable_count,
            terminal_count: self.terminal_count,
        }
    }

    pub(super) fn set_scheduler_state(&mut self, state: watch::Receiver<SchedulerState>) {
        self.scheduler_state = state;
    }

    pub(super) fn release_timing(&self) -> PrivateReleaseTiming {
        PrivateReleaseTiming::new(
            self.core.clock().clone(),
            self.release_deadlines.subscribe(),
        )
    }

    #[cfg(test)]
    pub(super) fn schedule(&self, admission_id: AdmissionId) -> Option<(u64, u64)> {
        self.core
            .snapshot()
            .admissions
            .into_iter()
            .find(|admission| admission.admission_id == admission_id)
            .map(|admission| (admission.accepted_at_ns, admission.scheduled_release_at_ns))
    }

    fn remove_reservation(&mut self, admission_id: AdmissionId) -> Option<Reservation> {
        let reservation = self.reservations.remove(&admission_id)?;
        self.admission_by_transaction
            .remove(&reservation.transaction.id());
        self.reservation_bytes -= reservation.serialized_bytes;
        Some(reservation)
    }

    fn publish_release_deadline(&self) {
        self.release_deadlines
            .send_replace(self.core.earliest_release_at());
    }
}

pub(super) enum PrivateReservationOutcome {
    Accepted(oneshot::Receiver<Result<(), BoxError>>),
    Existing,
}

#[cfg(test)]
mod tests;
