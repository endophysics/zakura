//! Clock and policy access behavior.

mod common;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionId, AdmissionOrigin, Clock, ManualClock, Timestamp,
};

#[test]
fn clock_returns_the_configured_clock() {
    // Given: a core with a configured counting clock.
    let clock = CountingClock::new(Timestamp(12));
    clock.set(Timestamp(13));
    let core = AdmissionCore::new(clock, policy());

    // When: the clock is inspected through the access seam.
    let observed = core.clock().now();

    // Then: the configured clock value is returned.
    assert_eq!(observed, Timestamp(13));
    assert_eq!(core.clock().calls(), 1);
}

#[test]
fn clock_mutation_changes_the_next_admission_observation() {
    // Given: an empty core with a manual clock.
    let mut core = AdmissionCore::new(ManualClock::new(Timestamp(12)), policy());

    // When: the owned clock is advanced before admission.
    core.clock_mut().set(Timestamp(21));
    core.admit(AdmissionId(7), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");

    // Then: the admission observes the mutated clock.
    assert_eq!(
        core.get(AdmissionId(7))
            .expect("admission exists")
            .state
            .accepted_at(),
        Timestamp(21)
    );
}

#[test]
fn policy_returns_the_configured_release_policy() {
    // Given: a core with a validated release policy.
    let configured = policy();
    let core = AdmissionCore::new(ManualClock::new(Timestamp(12)), configured);

    // When: the policy is inspected through the access seam.
    let observed = core.policy();

    // Then: the original policy is returned by reference.
    assert_eq!(observed, &configured);
}
