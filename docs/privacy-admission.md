# Private Admission Operator Guide

Private admission is available only in a `zakurad` binary compiled with the
`privacy-admission` feature. It keeps verified private submissions in a bounded,
volatile pool and releases them on fixed release epochs. Outside isolated regtest,
the RPC listener must use cookie authentication. This prevents unauthenticated
clients from submitting transactions to the private pool or reading its aggregate
diagnostics. Zakura generates a fresh credential at startup, stores it in an
owner-only file, and removes it on shutdown.

## Startup Records

After configuration validation succeeds, a feature-on process emits one structured
info record with `event="operator_privacy_policy"`. Its stable fields are:

- `policy_version` and `policy_hash`;
- `max_private_transactions` and `max_private_serialized_bytes`;
- `release_timing`, plus exact integer seconds and nanoseconds for the release epoch,
  minimum delay, and maximum delay;
- `egress="common_randomized_peer_set"`;
- `peer_diversity="per_ip_and_target_size"`;
- the existing `max_connections_per_ip` and `peerset_initial_target_size` controls.

Build identity is a separate `event="build_identity"` record. It is not part of the
privacy policy hash. Zakura does not report an upstream-base revision in either
record; deployment integration metadata owns that value.

## Policy Hash Scope

The hash is the lowercase hexadecimal SHA-256 digest of the versioned canonical
encoding specified by [ADR 0008](decisions/zakura/0008-operator-privacy-policy-hash.md).
It includes the private count and byte limits, release timing and delays, the
compile-time enabled marker, fixed-epoch timing, common randomized peer-set egress,
the per-IP/target-size diversity policy, `network.max_connections_per_ip`, and
`network.peerset_initial_target_size`. Both numeric controls are big-endian `u64`
inputs in policy version 1, so changing either control changes the policy hash.

The hash excludes build version, commit and upstream-base metadata, node and peer
identity, runtime counts, timestamps, and every transaction or admission identity.
It is an operator comparison fingerprint, not a consensus commitment, protocol
field, authentication mechanism, or generic configuration hash.

## Logical Egress and Diversity

Common randomized peer-set egress is logical egress. Private promotion joins the
same pending transaction-gossip path used by other verified transactions, and the
available peer set is selected by the common randomized peer-set machinery. Private
origin does not select a dedicated physical peer, route, or peer-selection class.

`max_connections_per_ip` and `peerset_initial_target_size` constrain the available
peer population. Neither value is a per-transaction routing control. The policy
record therefore does not guarantee delivery, a particular physical next hop,
independent network paths, latency, anonymity, or a minimum number of diverse peers
at any instant.

## Aggregate Telemetry

`getprivatepoolinfo` exposes existing aggregate pool diagnostics and nullable
`completed_window { promoted, recoverable, terminal }` counts. The completed window
is computed lazily when diagnostics are requested or an outcome is recorded. Windows
are non-overlapping release-epoch intervals, and only the most recently completed
interval is published.

The current partial window is never exposed. The response contains no window
timestamps, transaction IDs, admission IDs, or per-admission latencies. If observation
skips one or more epochs, the published value is the immediately preceding zero
window, not an older active window and not a cumulative total.

## Runtime and Restart Semantics

The startup policy record describes validated configuration and compile-time behavior.
It does not prove that the pool is currently nonempty, that a particular submission
will remain until its sampled release time, that peers are reachable, or that runtime
scheduling and network delivery will meet the configured timing bounds.

Private pool contents, admission state, and incomplete telemetry are memory-only.
Stopping, crashing, or restarting `zakurad` discards unreleased private submissions
and their admission state. After restart, clients must submit again if appropriate;
the new process emits a fresh startup policy record and starts with no completed
telemetry window.

## Compile-Time Rollback

There is no supported runtime `enabled=false` switch. Roll back private admission
exactly as follows:

1. Stop `zakurad` and account for the loss of all unreleased private submissions.
2. Remove the entire `[mempool.private_pool]` section from `zakura.toml`.
3. Rebuild without `privacy-admission`:

   ```sh
   cargo build --release --locked -p zakura --no-default-features \
     --features default-release-binaries
   ```

4. Deploy the resulting `target/release/zakurad` binary and restart the node.
5. Verify both private RPC methods are absent. With `RPC_URL` and `COOKIE_FILE` set
   for the restarted node, this check requires `curl` and `jq` and exits nonzero
   unless each call returns JSON-RPC method-not-found code `-32601`:

   ```sh
   for method in sendprivatetransaction getprivatepoolinfo; do
     code="$(curl --silent --show-error --user "$(cat "$COOKIE_FILE")" \
       --header 'content-type: application/json' \
       --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":[]}" \
       "$RPC_URL" | jq -r '.error.code')"
     test "$code" = -32601 || exit 1
   done
   ```

Do not retain private-pool configuration and do not substitute a runtime false value;
feature-off rollback is compile-time removal followed by a process restart.
