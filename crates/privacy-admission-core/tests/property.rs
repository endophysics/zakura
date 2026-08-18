//! Bounded operation-sequence properties for the admission state machine.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionOutcome,
    AdmissionStateLabel, BatchId, DiagnosticAdmission, DiagnosticSnapshot, ReasonCode,
    ReleaseOutcome, Timestamp,
};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Operation {
    Admit { id: u8, development: bool },
    Advance(u8),
    Refresh,
    Reject(u8),
    Remove(u8),
    Release,
    Rollback,
}

fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
        5 => (0..8u8, any::<bool>())
            .prop_map(|(id, development)| Operation::Admit { id, development }),
        3 => (0..20u8).prop_map(Operation::Advance),
        2 => Just(Operation::Refresh),
        1 => (0..8u8).prop_map(Operation::Reject),
        1 => (0..8u8).prop_map(Operation::Remove),
        2 => Just(Operation::Release),
        1 => Just(Operation::Rollback),
    ]
}

fn operation_sequence_strategy() -> impl Strategy<Value = Vec<Operation>> {
    proptest::collection::vec(operation_strategy(), 1..40)
}

struct Harness {
    core: AdmissionCore<CountingClock>,
    clock: CountingClock,
    now: u64,
    last_batch: Option<BatchId>,
}

impl Harness {
    fn new() -> Self {
        let clock = CountingClock::new(Timestamp(100));
        let mut core = AdmissionCore::new(clock.clone(), policy());
        core.admit(AdmissionId(0), AdmissionOrigin::PrivateGateway)
            .expect("fixture admission succeeds");
        Self {
            core,
            clock,
            now: 100,
            last_batch: None,
        }
    }

    fn apply(&mut self, operation: Operation) {
        let before = self.core.snapshot();
        match operation {
            Operation::Admit { id, development } => self.admit(&before, id, development),
            Operation::Advance(delta) => {
                self.now = self.now.saturating_add(u64::from(delta));
                self.clock.set(Timestamp(self.now));
            }
            Operation::Refresh => self.refresh(&before),
            Operation::Reject(id) => {
                let _result = self.core.reject(AdmissionId(u64::from(id)), reason());
            }
            Operation::Remove(id) => {
                let _result = self.core.remove(AdmissionId(u64::from(id)), reason());
            }
            Operation::Release => self.release(),
            Operation::Rollback => self.rollback(&before),
        }
        self.assert_invariants(&before);
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

    fn release(&mut self) {
        let outcome = self.core.release_due().expect("monotonic release succeeds");
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

    fn assert_invariants(&self, before: &DiagnosticSnapshot) {
        let after = self.core.snapshot();
        assert!(after
            .admissions
            .windows(2)
            .all(|pair| pair[0].admission_id < pair[1].admission_id));
        for prior in &before.admissions {
            let current = after
                .admissions
                .iter()
                .find(|record| record.admission_id == prior.admission_id)
                .expect("existing record is never deleted");
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

proptest! {
    #[test]
    fn bounded_operation_sequences_preserve_state_machine_invariants(
        operations in operation_sequence_strategy(),
    ) {
        // Given: one core with a known accepted clock marker.
        let mut harness = Harness::new();

        // When: a bounded mixed operation sequence executes.
        for operation in operations {
            harness.apply(operation);
        }

        // Then: every step has asserted retry, transition, rollback, and batch invariants.
    }
}
