use std::{cell::Cell, rc::Rc, time::Duration};

use super::*;

#[derive(Clone)]
struct TestClock(Rc<Cell<Timestamp>>);

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        self.0.get()
    }
}

#[test]
fn batch_overflow_leaves_clock_marker_and_all_states_unchanged() {
    // Given: a due admission with the next batch identifier exhausted.
    let clock = TestClock(Rc::new(Cell::new(Timestamp(12))));
    let policy = ReleasePolicy::new(
        Duration::from_nanos(10),
        Duration::from_nanos(5),
        Duration::from_nanos(25),
    );
    assert!(policy.is_ok(), "test policy must be valid");
    let Some(policy) = policy.ok() else {
        return;
    };
    let mut core = AdmissionCore::new(clock.clone(), policy);
    assert!(core
        .admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .is_ok());
    clock.0.set(Timestamp(20));
    core.next_batch_id = u64::MAX;
    let before = core.snapshot();
    let marker = core.last_observed;

    // When: releasing would overflow the next batch identifier.
    let result = core.release_due();

    // Then: overflow is atomic across the clock marker and every record.
    assert_eq!(result, Err(AdmissionError::BatchIdExhausted));
    assert_eq!(core.last_observed, marker);
    assert_eq!(core.snapshot(), before);
    assert_eq!(core.next_batch_id, u64::MAX);
}
