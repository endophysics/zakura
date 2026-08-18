use std::{cell::Cell, rc::Rc, time::Duration};

use privacy_admission_core::{Clock, ReleasePolicy, Timestamp};

#[derive(Clone, Debug)]
pub struct CountingClock {
    now: Rc<Cell<Timestamp>>,
    calls: Rc<Cell<u64>>,
}

impl CountingClock {
    pub fn new(now: Timestamp) -> Self {
        Self {
            now: Rc::new(Cell::new(now)),
            calls: Rc::new(Cell::new(0)),
        }
    }

    pub fn set(&self, now: Timestamp) {
        self.now.set(now);
    }

    pub fn calls(&self) -> u64 {
        self.calls.get()
    }
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.set(self.calls.get() + 1);
        self.now.get()
    }
}

pub fn policy() -> ReleasePolicy {
    ReleasePolicy::new(
        Duration::from_nanos(10),
        Duration::from_nanos(5),
        Duration::from_nanos(25),
    )
    .expect("test policy is valid")
}
