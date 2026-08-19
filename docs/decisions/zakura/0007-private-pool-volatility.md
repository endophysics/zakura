---
status: accepted
date: 2026-08-19
builds-on: [Admission and release core scheduling and crate boundary](0003-admission-release-core.md), [Propagate explicit admission context through local transaction plumbing](0004-admission-context-plumbing.md), [Store private verified transactions in a separate bounded pool](0005-private-verified-pool.md), [Promote complete private batches atomically](0006-private-promotion-semantics.md)
---

# Keep private pool state volatile and diagnostics aggregate

## Context and Problem Statement

[WP04](../../../../agent_packages/packages/WP04-zakura-private-pool.md) needs deterministic restart and shutdown behavior plus enough diagnostics to operate the private pool without exposing admission details. ADR 0003 defines in-memory admission state, and ADR 0004 defines the private context carried to it. Neither decision grants a persistence or public diagnostics boundary.

## Priorities & Constraints

- Shutdown and restart behavior must be deterministic.
- Diagnostics must show capacity and scheduler health without becoming a transaction or timing oracle.
- WP04 must not add durable private transaction or admission storage.

## Considered Options

- Persist private pool and admission-core state across restart.
- Treat all private pool and admission-core contents as volatile process state.

Persistence would require a storage format, recovery protocol, confidentiality policy, and atomic recovery relationship with public promotion. WP04 defines none of those contracts.

## Decision Outcome

Keep private transaction data, admission-core state, prepared releases, and scheduler state in memory only. An orderly shutdown stops new private admission and promotion, cancels and joins the scheduler, and then drops all remaining private contents. Process failure or restart also starts with an empty private pool and empty admission-core state. WP04 performs no recovery and writes no private pool persistence.

Expose diagnostics only as aggregate counts, aggregate bytes, counts by lifecycle state, configured limits, and scheduler health. Scheduler health may report whether it is running, stopping, or stalled and aggregate success or failure counters.

Diagnostics, logs, and metrics must not expose transaction IDs, admission IDs, hashes, transaction plaintext or bytes, or per-admission timestamps. This redaction applies in healthy, failure, shutdown, and restart paths.

## Expected Consequences

- Shutdown and restart discard unreleased private submissions in one defined way.
- Operators can detect pressure and scheduler failure without identifying a submission or reconstructing its exact timing.
- ADR 0005's isolation survives operational tooling, and ADR 0006's scheduler has a bounded aggregate health surface.
- Durable recovery is outside WP04.
