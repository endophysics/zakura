---
status: accepted
date: 2026-08-18
---

# Admission and release core scheduling and crate boundary

## Context and Problem Statement

The private-admission path needs a deterministic admission and release state machine that can be reviewed and tested without a running Zakura node. It must define time, retries, deadlines, terminal states, and due batches without taking ownership of transaction data or node integration.

## Priorities & Constraints

- The core must be a node-independent, synchronous crate with no Zakura-specific types or async runtime dependency.
- Admission identifiers and origin classes are opaque values. The core assigns no transaction, wallet, network, or storage meaning to them.
- Time comes from an injected deterministic clock. A clock observation earlier than the last accepted observation is rejected without mutating state.
- Diagnostic projections may expose opaque identifiers, origins, states, timestamps, deadlines, and batch identifiers, but never transaction plaintext or bytes.

## Considered Options

- Fixed epochs: group releases at configured epoch boundaries.
- Rolling pools: form release groups from a moving window or pool.

Fixed epochs have a smaller state and timing surface, make boundary cases reproducible, and produce deterministic batches from the same admissions and clock observations. Rolling pools may improve later traffic-shaping behavior, but add pool membership and window policy that the initial implementation does not need.

## Decision Outcome

Use a synchronous, node-independent admission core with fixed-epoch scheduling. Rolling pools are deferred.

Acceptance is an event, not a durable `Accepted` state. A successful acceptance records the original acceptance time and moves the admission to `Embargoed`. After its embargo and release deadline are satisfied, it becomes `Eligible`; a due batch moves it to `Released`. Policy actions may instead move it to `Rejected` or `Removed`. `Released`, `Rejected`, and `Removed` are absorbing terminal states.

Retries for an existing admission identifier are idempotent. They don't create another admission, reset `accepted_at`, change the release deadline, repeat a release, or move an admission out of a terminal state.

For epoch size `epoch`, minimum delay `minimum_delay`, and maximum delay `maximum_delay`, scheduling is:

```text
release_at = min(next_epoch(accepted_at + minimum_delay), accepted_at + maximum_delay)
```

`next_epoch(t)` returns the first fixed epoch boundary at or after `t`. If `t` is exactly on a boundary, it returns that boundary rather than the following one. This exact-boundary rule and the maximum-delay cap are part of the policy, so the same inputs always produce the same deadline.

At each accepted clock observation, all due admissions form an atomic batch with deterministic membership and ordering. The complete batch transitions to `Released` together, or no member does. Repeating the operation has no effect on already released admissions. This atomicity applies only to the in-memory state transition, not to delivery or persistence.

## Expected Consequences

- Tests can reproduce admission, embargo, eligibility, batching, rejection, removal, retries, epoch boundaries, maximum-delay caps, and clock rollback without node services.
- Implementations can be reviewed against one precise state machine and deadline formula.
- Fixed epochs provide deterministic initial behavior while deferring rolling-pool policy.
- The core does not own storage; transaction representation or parsing; consensus or mempool-policy verification; mempool or P2P integration; OHTTP; TEE support; wallet behavior; node plumbing; or delivery and persistence guarantees.
