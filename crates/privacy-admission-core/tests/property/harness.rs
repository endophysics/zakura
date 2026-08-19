use super::{
    common::{policy, CountingClock},
    Operation,
};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionOutcome,
    AdmissionStateLabel, BatchId, DiagnosticAdmission, DiagnosticSnapshot, PreparedRelease,
    ReasonCode, ReleaseOutcome, Timestamp,
};

pub(super) struct Harness {
    core: AdmissionCore<CountingClock>,
    clock: CountingClock,
    now: u64,
    last_batch: Option<BatchId>,
    prepared: Option<PreparedRelease>,
}

impl Harness {
    pub(super) fn new() -> Self {
        let clock = CountingClock::new(Timestamp(100));
        let mut core = AdmissionCore::new(clock.clone(), policy());
        core.admit(AdmissionId(0), AdmissionOrigin::PrivateGateway)
            .expect("fixture admission succeeds");
        Self {
            core,
            clock,
            now: 100,
            last_batch: None,
            prepared: None,
        }
    }

    pub(super) fn apply(&mut self, operation: Operation) {
        let before = self.core.snapshot();
        let discarded = match operation {
            Operation::Admit { id, development } => {
                self.admit(&before, id, development);
                None
            }
            Operation::Advance(delta) => {
                self.now = self.now.saturating_add(u64::from(delta));
                self.clock.set(Timestamp(self.now));
                None
            }
            Operation::Refresh => {
                self.refresh(&before);
                None
            }
            Operation::Reject(id) => {
                let _result = self.core.reject(AdmissionId(u64::from(id)), reason());
                None
            }
            Operation::Remove(id) => {
                let _result = self.core.remove(AdmissionId(u64::from(id)), reason());
                None
            }
            Operation::Discard(id) => self.discard(id),
            Operation::Prepare => {
                self.prepare(&before);
                None
            }
            Operation::Commit => {
                self.commit();
                None
            }
            Operation::Abort => {
                self.prepared = None;
                None
            }
            Operation::Release => {
                self.release();
                None
            }
            Operation::Rollback => {
                self.rollback(&before);
                None
            }
        };
        self.assert_invariants(&before, discarded);
    }

    fn admit(&mut self, before: &DiagnosticSnapshot, id: u8, development: bool) {
        let admission_id = AdmissionId(u64::from(id));
        let origin = if development {
            AdmissionOrigin::Development
        } else {
            AdmissionOrigin::PrivateGateway
        };
        let existing = before
            .admissions
            .iter()
            .find(|record| record.admission_id == admission_id);
        let calls = self.clock.calls();
        let result = self.core.admit(admission_id, origin);
        if let Some(record) = existing {
            assert_eq!(self.clock.calls(), calls);
            if record.origin == origin {
                assert!(matches!(result, Ok(AdmissionOutcome::Existing(_))));
            } else {
                assert!(matches!(
                    result,
                    Err(AdmissionError::ConflictingOrigin { .. })
                ));
            }
        } else {
            assert!(matches!(result, Ok(AdmissionOutcome::Accepted(_))));
            assert_eq!(self.clock.calls(), calls + 1);
        }
    }

    fn refresh(&mut self, before: &DiagnosticSnapshot) {
        let expected = before
            .admissions
            .iter()
            .filter(|record| {
                record.state == AdmissionStateLabel::Embargoed
                    && record.scheduled_release_at_ns <= self.now
            })
            .map(|record| record.admission_id)
            .collect::<Vec<_>>();
        let calls = self.clock.calls();
        let transitioned = self.core.refresh().expect("monotonic refresh succeeds");
        assert_eq!(self.clock.calls(), calls + 1);
        assert_eq!(transitioned, expected);
        assert!(transitioned
            .iter()
            .all(|admission_id| self.core.eligible_ids().contains(admission_id)));
    }

    fn discard(&mut self, id: u8) -> Option<AdmissionId> {
        let calls = self.clock.calls();
        let discarded = self
            .core
            .discard_uncommitted(AdmissionId(u64::from(id)))
            .ok()
            .map(|view| view.admission_id);
        assert_eq!(self.clock.calls(), calls);
        discarded
    }

    fn release(&mut self) {
        let outcome = self.core.release_due().expect("monotonic release succeeds");
        self.record_release(outcome);
    }

    fn prepare(&mut self, before: &DiagnosticSnapshot) {
        let calls = self.clock.calls();
        let prepared = self
            .core
            .prepare_release()
            .expect("monotonic preparation succeeds");
        assert_eq!(self.clock.calls(), calls + 1);
        assert_eq!(self.core.snapshot(), *before);
        if let Some(prepared) = &prepared {
            let mut expected = before
                .admissions
                .iter()
                .filter(|record| {
                    record.state == AdmissionStateLabel::Eligible
                        || (record.state == AdmissionStateLabel::Embargoed
                            && record.scheduled_release_at_ns <= self.now)
                })
                .map(|record| (record.scheduled_release_at_ns, record.admission_id))
                .collect::<Vec<_>>();
            expected.sort_unstable();
            assert_eq!(
                prepared.admission_ids(),
                expected
                    .iter()
                    .map(|(_, admission_id)| *admission_id)
                    .collect::<Vec<_>>()
            );
        }
        self.prepared = prepared;
    }

    fn commit(&mut self) {
        let Some(prepared) = self.prepared.take() else {
            return;
        };
        let before = self.core.snapshot();
        let calls = self.clock.calls();
        match self.core.commit_release(prepared) {
            Ok(outcome) => self.record_release(outcome),
            Err(AdmissionError::StalePreparedRelease) => assert_eq!(self.core.snapshot(), before),
            Err(error) => panic!("unexpected commit error: {error}"),
        }
        assert_eq!(self.clock.calls(), calls);
    }

    fn record_release(&mut self, outcome: ReleaseOutcome) {
        if let ReleaseOutcome::Released {
            batch_id,
            admissions,
        } = outcome
        {
            if let Some(previous) = self.last_batch {
                assert_eq!(batch_id.0, previous.0 + 1);
            } else {
                assert_eq!(batch_id, BatchId(0));
            }
            assert!(admissions.windows(2).all(|pair| {
                (pair[0].state.scheduled_release_at(), pair[0].admission_id)
                    <= (pair[1].state.scheduled_release_at(), pair[1].admission_id)
            }));
            self.last_batch = Some(batch_id);
        }
    }

    fn rollback(&mut self, before: &DiagnosticSnapshot) {
        let calls = self.clock.calls();
        self.clock.set(Timestamp(0));
        let result = self.core.release_due();
        self.clock.set(Timestamp(self.now));
        assert!(matches!(result, Err(AdmissionError::ClockRollback { .. })));
        assert_eq!(self.clock.calls(), calls + 1);
        assert_eq!(self.core.snapshot(), *before);
    }

    fn assert_invariants(&self, before: &DiagnosticSnapshot, discarded: Option<AdmissionId>) {
        let after = self.core.snapshot();
        assert!(after
            .admissions
            .windows(2)
            .all(|pair| pair[0].admission_id < pair[1].admission_id));
        for prior in &before.admissions {
            if discarded == Some(prior.admission_id) {
                assert!(!terminal(prior));
                assert!(after
                    .admissions
                    .iter()
                    .all(|record| record.admission_id != prior.admission_id));
                continue;
            }
            let current = after
                .admissions
                .iter()
                .find(|record| record.admission_id == prior.admission_id)
                .expect("existing record is never deleted without discard");
            assert_eq!(current.accepted_at_ns, prior.accepted_at_ns);
            assert_eq!(
                current.scheduled_release_at_ns,
                prior.scheduled_release_at_ns
            );
            if terminal(prior) {
                assert_eq!(current, prior);
            }
        }
    }
}

fn reason() -> ReasonCode {
    ReasonCode::try_from("property").expect("reason is valid")
}

fn terminal(record: &DiagnosticAdmission) -> bool {
    matches!(
        record.state,
        AdmissionStateLabel::Released
            | AdmissionStateLabel::Rejected
            | AdmissionStateLabel::Removed
    )
}
