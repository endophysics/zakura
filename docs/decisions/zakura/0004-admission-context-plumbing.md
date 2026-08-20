---
status: accepted
date: 2026-08-19
builds-on: [Admission and release core scheduling and crate boundary](0003-admission-release-core.md)
---

# Propagate explicit admission context through local transaction plumbing

## Context and Problem Statement

Zakura's transaction admission boundary must distinguish private local submissions from peer, crawler, and legacy local traffic. That distinction must survive download, verification, cancellation, and retry without changing verification results or the ordinary post-verification path.

## Priorities & Constraints

- Every submission is classified explicitly as peer, crawler, legacy local, or private local.
- Private admission plumbing is gated by a Cargo feature that is disabled by default.
- The change must remain additive and concentrated in request types, pending download state, completion results, and retry representation.
- Verifier behavior, ordinary mempool insertion, and gossip behavior remain unchanged.

## Considered Options

- Propagate typed origin and policy context with each transaction.
- Infer private admission policy from an existing generic request or queue path.

Inference would make policy depend on which caller happened to use a generic transport shape. That relationship is implicit, can be lost when a transaction is retried or requeued, and can't reliably distinguish legacy local traffic from private local traffic. Explicit propagation makes the admission decision visible at the boundary and preserves it across every ownership transfer.

## Decision Outcome

Add explicit crawler and private-local requests without changing existing peer or legacy-local requests. A generic request is never interpreted as private and carries no inferred private policy.

Private-local submissions carry an `AdmissionContext` containing a typed `AdmissionId` and a fixed-epoch policy selector. Zakura assigns the identifier internally and never accepts or returns it through the JSON-RPC boundary. The selector identifies the policy chosen at admission; it does not move scheduling decisions into request handling, downloading, or verification.

The private submission RPC is an internal gateway-to-node adapter, not a wallet-facing endpoint. An exact transaction retry is idempotent even though each RPC attempt receives a fresh internal identifier: the first accepted identity remains canonical. An `Existing` result reveals only that the exact submitted transaction is already retained or in flight; callers cannot probe private-pool membership by choosing admission identifiers. Zakura requires RPC cookie authentication outside regtest; unauthenticated private submission is limited to isolated regtest inspection.

The downloader owns one complete pending record containing the transaction and its admission context. That record, rather than a separate lookup or inference step, is the source of context for verification completion, cancellation, and chain-tip retry or requeue. Completion returns the same context as opaque metadata while verifier rules and outcomes remain unchanged.

Accepted transactions continue through ordinary mempool insertion and existing gossip. Removing a transaction, including cancellation and terminal removal, must remove its associated context metadata so stale admission state can't outlive the transaction.

## Expected Consequences

- The acceptance boundary can distinguish all four origins without treating generic local traffic as private.
- Private admission identity and policy survive successful completion and retry because they remain part of the downloader-owned pending record.
- Feature-disabled builds retain the existing path.
- This decision adds context plumbing only. It doesn't define release arithmetic, alter relay behavior, or create a new insertion path.
