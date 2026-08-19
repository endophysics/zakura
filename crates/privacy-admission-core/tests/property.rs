//! Bounded operation-sequence properties for the admission state machine.

mod common;
#[path = "property/harness.rs"]
mod harness;

use harness::Harness;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Operation {
    Admit { id: u8, development: bool },
    Advance(u8),
    Refresh,
    Reject(u8),
    Remove(u8),
    Discard(u8),
    Prepare,
    Commit,
    Abort,
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
        1 => (0..8u8).prop_map(Operation::Discard),
        2 => Just(Operation::Prepare),
        2 => Just(Operation::Commit),
        1 => Just(Operation::Abort),
        1 => Just(Operation::Release),
        1 => Just(Operation::Rollback),
    ]
}

fn operation_sequence_strategy() -> impl Strategy<Value = Vec<Operation>> {
    proptest::collection::vec(operation_strategy(), 1..40)
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

        // Then: every step has asserted retry, transition, rollback, discard, and batch invariants.
    }
}
