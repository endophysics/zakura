---
status: accepted
date: 2026-08-19
builds-on: [Admission and release core scheduling and crate boundary](0003-admission-release-core.md), [Propagate explicit admission context through local transaction plumbing](0004-admission-context-plumbing.md)
---

# Store private verified transactions in a separate bounded pool

## Context and Problem Statement

[WP04](../../../../agent_packages/packages/WP04-zakura-private-pool.md) replaces WP03's public insertion of verified private submissions with isolated retention until release. ADR 0003 defines scheduling without owning transactions, and ADR 0004 carries private admission context through verification. Zakura now needs an owner for verified transaction data that cannot expose or influence it through public mempool behavior.

## Priorities & Constraints

- Before release, a private transaction must have no effect on public lookup, inventory, counts, byte totals, dependency graphs, conflicts, eviction, block-template selection, indexer events, gossip, or transaction-specific telemetry.
- Private storage must have independent, bounded count and byte limits.
- The first implementation must define private dependency handling without adding dependency-closed promotion.

## Considered Options

- Store verified private transactions in a physically separate in-memory pool.
- Insert them into the public mempool with private visibility flags and filter every public surface.

Visibility flags would make privacy depend on every current and future public-mempool reader honoring the flag. A separate owner makes absence from public state the default and keeps private resource policy from changing public policy.

## Decision Outcome

Use a physically separate, bounded, in-memory private verified pool. A verified private transaction and its admission context enter this pool instead of ordinary public storage. Private retention is never represented by public visibility flags. The configured maximum transaction count bounds the total of admission-core records, including active and terminal records, plus in-flight reservations. Terminal records are absorbing for the process lifetime, so exhaustion remains until the state is dropped on shutdown or restart.

Enforce the total private admission record-count bound separately from the serialized-byte bound. The count bound accounts for admission-core records and reservations; the byte bound accounts for retained pool bytes and reservation bytes. Capacity pressure may reject a new private candidate or terminally evict a private candidate according to private policy, but it cannot inspect, displace, or otherwise evict a public entry.

Reject private admission when the transaction depends on an unconfirmed private parent. WP04 does not implement dependency-closed private retention or promotion.

## Expected Consequences

- Pre-release private transactions are absent from public storage and all public behavior by construction.
- Private admission consumes bounded memory without competing through public eviction policy.
- Transactions with unconfirmed private parents require a later submission after the parent is confirmed or public; no private dependency graph is created.
- ADR 0003 remains the scheduling authority, and ADR 0004 remains the source of private admission context.
