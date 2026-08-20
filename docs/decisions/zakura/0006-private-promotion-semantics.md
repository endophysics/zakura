---
status: accepted
date: 2026-08-19
builds-on: [Admission and release core scheduling and crate boundary](0003-admission-release-core.md), [Propagate explicit admission context through local transaction plumbing](0004-admission-context-plumbing.md), [Store private verified transactions in a separate bounded pool](0005-private-verified-pool.md)
---

# Promote complete private batches atomically

## Context and Problem Statement

Due transactions must move from the private pool into Zakura's unchanged public insertion and gossip path. ADR 0003 makes a due batch atomic in the admission core, while ADR 0004 preserves admission identity through retries and chain-tip changes. The node integration must extend that atomicity across public promotion and define which failures consume private state.

## Priorities & Constraints

- Duplicate retry and chain-tip reset must not weaken the minimum or maximum release timing selected at original acceptance.
- Promotion must not expose a partial due batch.
- Public insertion remains the only point that enables existing public events and gossip.
- Waiting for a deadline must not occupy a Tower service call.

## Considered Options

- Promote each candidate independently and reconcile partial results afterward.
- Preflight and commit one complete due batch through one atomic public adapter operation.

Per-candidate insertion can expose a prefix of a batch and leave public and core state inconsistent. One batch operation gives the adapter enough information to reject without mutation or commit the exact set.

## Decision Outcome

Keep the original `accepted_at` and release deadline across an exact duplicate retry and a chain-tip reset. A tip reset may invalidate contextual verification, but it never restarts the embargo or recalculates the deadline.

Run a separate cancellation-aware scheduler that observes deadlines and tip changes, then invokes release work. Tower admission and verification service calls never sleep until a release time.

For each release attempt:

1. Call the ADR 0003 core's `prepare_release` to obtain an opaque `PreparedRelease` for the complete due batch. Preparation is nonterminal and consumes no batch identifier.
2. Reverify all contextual conditions against the current chain tip and public state.
3. Send the exact complete batch through one synchronous public adapter operation. The adapter preflights every candidate, then either commits every insertion or leaves public state unchanged.
4. After public commit succeeds, immediately call `commit_release` with the matching preparation. Only this core commit records the batch as released and terminal.
5. Remove private copies only after successful public commit and core commit, or after a terminal result defined below.

Transient verifier or state unavailability and tip staleness are recoverable. They cause no public mutation or core commit, retain every affected private copy with its original deadline, discard the nonterminal preparation, and allow a fresh preparation and revalidation later.

Deterministic policy or consensus rejection, public conflict, expiry, and private candidate eviction are terminal for the affected candidate. Before any public commit, a terminal preflight result aborts the complete batch with no public mutation. The affected candidate is recorded as `Rejected` for policy or consensus failure, or `Removed` for conflict, expiry, or eviction, and its private copy is deleted. Other candidates remain private and eligible for a newly prepared complete due batch. No failed preparation consumes a batch identifier.

## Expected Consequences

- A due batch becomes public as one unit and enters existing Zakura events and gossip only after the adapter commits it.
- Core release state cannot lead public state because `commit_release` follows public commit.
- Recoverable failures preserve candidates and timing, while terminal failures remove only candidates that cannot be promoted.
- Tip changes force fresh contextual checks without granting a new release delay.
