use std::collections::HashSet;

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::{AdmissionId, PrivatePromotionOutcome};

use super::{lifecycle::TerminalDecision, PrivateAdmissionState};
use crate::components::mempool::{
    storage::{
        AtomicBatchInsertError, ExactTipRejectionError, SameEffectsChainRejectionError,
        SameEffectsTipRejectionError, Storage,
    },
    MempoolError,
};

pub(super) const fn candidate_terminal_decision(error: &MempoolError) -> Option<TerminalDecision> {
    match error {
        MempoolError::StorageExactTip(ExactTipRejectionError::FailedVerification(_)) => {
            Some(TerminalDecision::reject("consensus_rejected"))
        }
        MempoolError::StorageExactTip(ExactTipRejectionError::FailedStandard(_))
        | MempoolError::NonStandardTransaction(_) => {
            Some(TerminalDecision::reject("policy_rejected"))
        }
        MempoolError::StorageEffectsTip(
            SameEffectsTipRejectionError::SpendConflict
            | SameEffectsTipRejectionError::MissingOutput,
        )
        | MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::DuplicateSpend)
        | MempoolError::InMempool => Some(TerminalDecision::remove("public_conflict")),
        MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Expired) => {
            Some(TerminalDecision::remove("expired"))
        }
        MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::Mined) => {
            Some(TerminalDecision::remove("mined"))
        }
        MempoolError::StorageEffectsChain(SameEffectsChainRejectionError::RandomlyEvicted)
        | MempoolError::AlreadyQueued
        | MempoolError::ConflictingPrivateAdmission
        | MempoolError::PrivateAdmissionIdConflict
        | MempoolError::TerminalPrivateAdmission
        | MempoolError::PrivatePoolFull
        | MempoolError::PrivateOperationsClosed
        | MempoolError::FullQueue
        | MempoolError::Disabled => None,
    }
}

pub(in crate::components::mempool) struct PrivatePromotionEffects {
    pub outcome: PrivatePromotionOutcome,
    pub accepted: HashSet<UnminedTxId>,
    pub evicted: HashSet<UnminedTxId>,
}

impl PrivatePromotionEffects {
    fn without_public_effects(outcome: PrivatePromotionOutcome) -> Self {
        Self {
            outcome,
            accepted: HashSet::new(),
            evicted: HashSet::new(),
        }
    }
}

impl PrivateAdmissionState {
    pub(in crate::components::mempool) fn promote_due(
        &mut self,
        storage: &mut Storage,
        excluded: &HashSet<AdmissionId>,
    ) -> PrivatePromotionEffects {
        let core_before = self.core.clone();
        let prepared = match self.core.prepare_release() {
            Ok(Some(prepared)) => prepared,
            Ok(None) => {
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::NoDue,
                )
            }
            Err(_) => {
                let count = self.pool.stats().transaction_count;
                self.core = core_before;
                self.recoverable_count += count;
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::Recoverable { count },
                );
            }
        };
        let count = prepared.admission_ids().len();
        if prepared
            .admission_ids()
            .iter()
            .any(|admission_id| excluded.contains(admission_id))
        {
            self.core = core_before;
            self.recoverable_count += count;
            return PrivatePromotionEffects::without_public_effects(
                PrivatePromotionOutcome::Recoverable { count },
            );
        }
        let batch = match self.snapshot_batch(prepared.admission_ids()) {
            Ok(batch) => batch,
            Err(_) => {
                self.core = core_before;
                self.recoverable_count += count;
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::Recoverable { count },
                );
            }
        };
        let mut committed_core = self.core.clone();
        if committed_core.commit_release(prepared).is_err() {
            self.core = core_before;
            self.recoverable_count += count;
            return PrivatePromotionEffects::without_public_effects(
                PrivatePromotionOutcome::Recoverable { count },
            );
        }
        let public_effects = match storage.insert_private_batch(&batch) {
            Ok(effects) => effects,
            Err(AtomicBatchInsertError::Candidate {
                admission_id,
                source,
            }) => {
                self.core = core_before;
                let Some(decision) = candidate_terminal_decision(&source) else {
                    self.recoverable_count += count;
                    return PrivatePromotionEffects::without_public_effects(
                        PrivatePromotionOutcome::Recoverable { count },
                    );
                };
                let terminal_count = match self.terminalize(&[(admission_id, decision)]) {
                    Ok(terminal_count) => terminal_count,
                    Err(_) => {
                        self.recoverable_count += count;
                        return PrivatePromotionEffects::without_public_effects(
                            PrivatePromotionOutcome::Recoverable { count },
                        );
                    }
                };
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::Terminal {
                        count: terminal_count,
                    },
                );
            }
            Err(AtomicBatchInsertError::BatchMemberEvicted { admission_ids }) => {
                self.core = core_before;
                let decisions = admission_ids
                    .into_iter()
                    .map(|admission_id| (admission_id, TerminalDecision::remove("private_evicted")))
                    .collect::<Vec<_>>();
                let terminal_count = match self.terminalize(&decisions) {
                    Ok(terminal_count) => terminal_count,
                    Err(_) => {
                        self.recoverable_count += count;
                        return PrivatePromotionEffects::without_public_effects(
                            PrivatePromotionOutcome::Recoverable { count },
                        );
                    }
                };
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::Terminal {
                        count: terminal_count,
                    },
                );
            }
            Err(AtomicBatchInsertError::EvictionInvariant) => {
                self.core = core_before;
                self.recoverable_count += count;
                return PrivatePromotionEffects::without_public_effects(
                    PrivatePromotionOutcome::Recoverable { count },
                );
            }
        };
        self.core = committed_core;
        self.pool.remove_validated_batch(batch);
        self.promoted_count += count;
        self.publish_release_deadline();
        PrivatePromotionEffects {
            outcome: PrivatePromotionOutcome::Promoted { count },
            accepted: public_effects.accepted,
            evicted: public_effects.evicted,
        }
    }
}
