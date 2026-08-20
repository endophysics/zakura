---
status: accepted
date: 2026-08-20
builds-on: [Admission and release core scheduling and crate boundary](0003-admission-release-core.md), [Propagate explicit admission context through local transaction plumbing](0004-admission-context-plumbing.md), [Store private verified transactions in a separate bounded pool](0005-private-verified-pool.md), [Keep private pool state volatile and diagnostics aggregate](0007-private-pool-volatility.md)
---

# Hash the active operator privacy policy with a canonical projection

## Context and Problem Statement

Operators need a stable fingerprint showing whether two Zakura processes use the same private-admission policy without exposing private transactions or treating an entire configuration file as policy. The fingerprint must change for every setting that affects private-pool capacity or temporal release and must also bind the fixed egress and peer-diversity behavior supplied by this build.

## Priorities & Constraints

- Project only validated `PrivatePoolConfig` and the validated peer-diversity controls; do not hash generic serialized configuration.
- Make the byte encoding independent of platform word size, serializer behavior, field names, and field ordering.
- Separate policy identity from build identity, process state, and private admission data.
- Make feature-off behavior a compile-time absence rather than a second runtime policy value.

## Considered Options

- Hash the complete serialized node configuration.
- Hash an explicit, versioned binary projection of privacy-policy inputs.
- Print the settings without a digest.

## Decision Outcome

Use a typed `OperatorPrivacyPolicy` projection explicitly constructed from validated `PrivatePoolConfig`, `network.max_connections_per_ip`, and `network.peerset_initial_target_size`. Construction checks that all four platform-sized limits fit unsigned 64-bit canonical fields. A failure identifies the transaction limit, serialized-byte limit, per-IP connection limit, or peer-set target with a typed error.

The projection includes these configurable inputs, in this order:

1. Maximum private transaction count.
2. Maximum private serialized bytes.
3. Fixed release epoch.
4. Minimum release delay.
5. Maximum release delay.
6. Maximum peer connections per IP.
7. Initial peer-set target size.

It also includes these compile-time policy facts:

- Private admission is enabled. The policy module exists only with the `privacy-admission` feature, and its enabled byte is always `1`; there is no enabled-false runtime policy or digest.
- Release timing uses the fixed-epoch policy.
- Every verified local submission, peer relay, and private promotion enters the same pending-gossip set and emits `AdvertiseTransactionIds(ids, None)`. Client or relay origin does not select a separate logical egress policy or a physical peer.
- `network.max_connections_per_ip` and `network.peerset_initial_target_size` are ordinary peer-diversity and capacity controls for the available randomized peer set. They are not per-transaction routing controls, and private origin does not create a distinct peer-selection class.

The SHA-256 input is exactly 107 bytes. Integers use big-endian encoding, and enum discriminants are one byte:

| Byte offset | Length | Value |
| --- | ---: | --- |
| 0 | 30 | ASCII `zakura.operator-privacy-policy` |
| 30 | 1 | NUL domain terminator, `0x00` |
| 31 | 4 | Policy version, unsigned `u32`, value `1` |
| 35 | 1 | Enabled, value `1` |
| 36 | 8 | Maximum private transaction count, unsigned `u64` |
| 44 | 8 | Maximum private serialized bytes, unsigned `u64` |
| 52 | 1 | Fixed-epoch release discriminant, value `1` |
| 53 | 8 | Release epoch whole seconds, unsigned `u64` |
| 61 | 4 | Release epoch subsecond nanoseconds, unsigned `u32` |
| 65 | 8 | Minimum release delay whole seconds, unsigned `u64` |
| 73 | 4 | Minimum release delay subsecond nanoseconds, unsigned `u32` |
| 77 | 8 | Maximum release delay whole seconds, unsigned `u64` |
| 85 | 4 | Maximum release delay subsecond nanoseconds, unsigned `u32` |
| 89 | 1 | Common-randomized-peer-set egress discriminant, value `1` |
| 90 | 1 | Per-IP-and-target-size diversity discriminant, value `1` |
| 91 | 8 | Maximum peer connections per IP, unsigned `u64` |
| 99 | 8 | Initial peer-set target size, unsigned `u64` |

Hash the complete byte sequence once with SHA-256 and display the 32-byte digest as 64 lowercase hexadecimal characters. For the test policy `(1000 transactions, 16777216 bytes, 60s+123ns epoch, 300s+456ns minimum, 600s+789ns maximum, 4 connections per IP, 75 initial peers)`, the digest is `29a1f5fdb33f5e3da1be6edd92eea5a9e3b04c59d39d0ae6043eecfb538e2552`.

The projection excludes commits, upstream revisions, build timestamps, node or peer IDs, transaction or admission IDs, runtime transaction counts, runtime byte counts, process timestamps, and all other build or runtime metadata. Its constructor has no API parameter through which those values can enter.

This digest is an operator inspection fingerprint, not a consensus commitment, network protocol field, cross-stack configuration schema, compatibility promise for generic configuration serialization, or authentication mechanism. This corrected shape remains policy version 1 because the earlier shape was unreleased within the same implementation cycle. After version 1 is released, a change to the canonical bytes requires a new policy version and updated test vector.

## Expected Consequences

- Equal complete private-pool and peer-diversity policy produces equal hashes across supported platforms.
- Every included configuration setting changes the hash while excluded metadata cannot perturb it.
- Unknown and invalid configuration still fails at the serde validation boundary before projection.
- Disabling `privacy-admission` removes the projection and its enabled policy at compile time.
