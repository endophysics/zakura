//! The Zakura mempool.
//!
//! A service that manages known unmined Zcash transactions.

use std::{collections::HashSet, fmt, net::SocketAddr};

use tokio::sync::oneshot;
use zakura_chain::{
    block,
    transaction::{self, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};

use crate::BoxError;

mod gossip;
mod mempool_change;
#[cfg(feature = "privacy-admission")]
mod private;
mod service_trait;
mod transaction_dependencies;

#[cfg(feature = "privacy-admission")]
pub use self::private::{
    AdmissionContext, AdmissionId, AdmissionPolicy, PrivateAdmissionStatus, PrivatePoolDiagnostics,
    PrivatePromotionOutcome, SchedulerState,
};
pub use self::{
    gossip::Gossip,
    mempool_change::{MempoolChange, MempoolChangeKind, MempoolTxSubscriber},
    service_trait::MempoolService,
    transaction_dependencies::TransactionDependencies,
};

/// The mempool is disabled until Zakura is close to the chain tip.
#[derive(Debug)]
pub struct MempoolDisabledError;

impl fmt::Display for MempoolDisabledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("mempool is not active: wait for Zakura to sync to the tip")
    }
}

impl std::error::Error for MempoolDisabledError {}

#[cfg(test)]
mod tests {
    use super::MempoolDisabledError;

    #[cfg(feature = "privacy-admission")]
    use super::{
        AdmissionContext, AdmissionId, AdmissionOrigin, AdmissionPolicy, PrivateAdmissionStatus,
        PrivatePromotionOutcome, Request, Response,
    };
    #[cfg(all(feature = "privacy-admission", feature = "rpc-client"))]
    use super::{PrivatePoolDiagnostics, SchedulerState};
    #[cfg(feature = "privacy-admission")]
    use zakura_chain::transaction::UnminedTx;

    #[test]
    fn mempool_disabled_error_uses_zakura_branding() {
        assert_eq!(
            MempoolDisabledError.to_string(),
            "mempool is not active: wait for Zakura to sync to the tip"
        );
    }

    #[cfg(feature = "privacy-admission")]
    #[test]
    fn private_queue_request_carries_context_and_requires_a_full_transaction() {
        // Given: a fixed-epoch private admission context.
        let context = AdmissionContext {
            admission_id: AdmissionId(7),
            policy: AdmissionPolicy::FixedEpoch,
        };

        // When: a private request constructor is assigned its public API shape.
        let _: fn(UnminedTx, AdmissionContext) -> Request =
            |transaction, context| Request::QueuePrivate {
                transaction,
                context,
            };

        // Then: the context is preserved in its canonical private-local origin.
        assert!(matches!(
            AdmissionOrigin::PrivateLocal(context),
            AdmissionOrigin::PrivateLocal(actual) if actual == context
        ));

        // Then: private admission and diagnostics have dedicated typed service shapes.
        let _: Request = Request::PrivatePoolDiagnostics;
        let _: fn(PrivateAdmissionStatus) -> Response = |status| Response::PrivateQueued {
            status,
            completion: None,
        };
    }

    #[cfg(all(feature = "privacy-admission", feature = "rpc-client"))]
    #[test]
    fn private_aggregate_contracts_serialize_without_private_identity_or_timing() {
        // Given: sentinel aggregate values and an accepted admission result.
        let status = PrivateAdmissionStatus::Accepted;
        let diagnostics = PrivatePoolDiagnostics {
            transaction_count: 3,
            serialized_bytes: 4096,
            max_transactions: 10,
            max_serialized_bytes: 8192,
            embargoed_count: 1,
            eligible_count: 1,
            releasing_count: 1,
            scheduler_state: SchedulerState::Idle,
            promoted_count: 7,
            recoverable_count: 2,
            terminal_count: 1,
        };
        let outcome = PrivatePromotionOutcome::Promoted { count: 3 };

        // When: the contracts are serialized and debug-formatted.
        let scheduler_states = [
            SchedulerState::Idle,
            SchedulerState::Running,
            SchedulerState::Stopping,
            SchedulerState::Stalled,
        ];
        let serialized = serde_json::to_string(&(status, diagnostics, outcome, scheduler_states))
            .expect("aggregate contracts serialize");
        let debug = format!("{status:?} {diagnostics:?} {outcome:?}");

        // Then: only aggregate, non-sensitive fields are exposed.
        for forbidden in [
            "transaction_id",
            "admission_id",
            "hash",
            "plaintext",
            "bytes_data",
            "accepted_at",
            "scheduled_release_at",
            "terminal_at",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "serialized field: {forbidden}"
            );
            assert!(!debug.contains(forbidden), "debug field: {forbidden}");
        }
        assert!(serialized.contains("transaction_count"));
        assert!(serialized.contains("serialized_bytes"));
        assert!(serialized.contains("\"scheduler_state\":\"idle\""));
        assert!(serialized.contains("\"running\""));
        assert!(serialized.contains("\"stopping\""));
        assert!(serialized.contains("\"stalled\""));
        assert!(serialized.contains("promoted_count"));
        assert!(serialized.contains("recoverable_count"));
        assert!(serialized.contains("terminal_count"));
    }

    #[cfg(feature = "privacy-admission")]
    #[test]
    fn private_promotion_outcomes_are_aggregate_only() {
        // Given: each internal promotion outcome shape.
        let outcomes = [
            PrivatePromotionOutcome::NoDue,
            PrivatePromotionOutcome::Promoted { count: 2 },
            PrivatePromotionOutcome::Recoverable { count: 1 },
            PrivatePromotionOutcome::Terminal { count: 4 },
        ];

        // When: outcomes are debug-formatted.
        let debug = outcomes
            .iter()
            .map(|outcome| format!("{outcome:?}"))
            .collect::<Vec<_>>()
            .join(" ");

        // Then: no outcome can carry a per-admission value.
        for forbidden in ["transaction", "admission", "hash", "plaintext", "timestamp"] {
            assert!(!debug.contains(forbidden), "debug field: {forbidden}");
        }
    }

    #[cfg(feature = "privacy-admission")]
    #[test]
    fn private_promotion_has_a_dedicated_service_contract() {
        // Given: the internal synchronous promotion request.
        let request = Request::PromotePrivateDue;

        // When: a successful aggregate outcome is assigned to its response shape.
        let response = Response::PrivatePromoted(PrivatePromotionOutcome::Promoted { count: 2 });

        // Then: request and response remain distinct from public queue admission.
        assert!(matches!(request, Request::PromotePrivateDue));
        assert!(matches!(
            response,
            Response::PrivatePromoted(PrivatePromotionOutcome::Promoted { count: 2 })
        ));
    }
}

/// A peer source for per-peer mempool download accounting.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QueueSource {
    /// A transaction advertisement from the legacy TCP transport.
    LegacySocket(SocketAddr),

    /// A transaction advertisement from an authenticated Zakura peer.
    ///
    /// Stored as the encoded Zakura peer id to keep this service-interface crate
    /// independent from the `zakura-network` transport types.
    Zakura(Vec<u8>),
}

impl From<SocketAddr> for QueueSource {
    fn from(source: SocketAddr) -> Self {
        Self::LegacySocket(source)
    }
}

/// The provenance of a transaction admission request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionOrigin {
    /// A request advertised by a peer.
    Peer(QueueSource),

    /// A request collected by the transaction crawler.
    Crawler,

    /// A request submitted through the existing local queue path.
    LegacyLocal,

    /// A request submitted through the feature-gated private local path.
    #[cfg(feature = "privacy-admission")]
    PrivateLocal(AdmissionContext),
}

/// A mempool service request.
///
/// Requests can query the current set of mempool transactions,
/// queue transactions to be downloaded and verified, or
/// run the mempool to check for newly verified transactions.
///
/// Requests can't modify the mempool directly,
/// because all mempool transactions must be verified.
#[derive(Debug, Eq, PartialEq)]
pub enum Request {
    /// Query all [`UnminedTxId`]s in the mempool.
    TransactionIds,

    /// Return and clear up to `limit` transaction IDs awaiting proactive
    /// advertisement through the peer set.
    ///
    /// This pending set is separate from the full mempool inventory returned
    /// by [`Request::TransactionIds`] in response to peer `mempool` requests.
    TakePendingGossipTransactionIds {
        /// Maximum number of transaction IDs to return.
        limit: usize,
    },

    /// Query matching [`UnminedTx`] in the mempool,
    /// using a unique set of [`UnminedTxId`]s.
    TransactionsById(HashSet<UnminedTxId>),

    /// Query matching [`UnminedTx`] in the mempool,
    /// using a unique set of [`transaction::Hash`]es. Pre-V5 transactions are matched
    /// directly; V5 transaction are matched just by the Hash, disregarding
    /// the [`AuthDigest`](zakura_chain::transaction::AuthDigest).
    TransactionsByMinedId(HashSet<transaction::Hash>),

    /// Request a [`transparent::Output`] identified by the given [`OutPoint`](transparent::OutPoint),
    /// waiting until it becomes available if it is unknown.
    ///
    /// This request is purely informational, and there are no guarantees about
    /// whether the UTXO remains unspent or is on the best chain, or any chain.
    /// Its purpose is to allow orphaned mempool transaction verification.
    ///
    /// # Correctness
    ///
    /// Output requests should be wrapped in a timeout, so that
    /// out-of-order and invalid requests do not hang indefinitely.
    ///
    /// Outdated requests are pruned on a regular basis.
    AwaitOutput(transparent::OutPoint),

    /// Request a [`VerifiedUnminedTx`] and its dependencies by its mined id.
    TransactionWithDepsByMinedId(transaction::Hash),

    /// Get all the [`VerifiedUnminedTx`] in the mempool.
    ///
    /// Equivalent to `TransactionsById(TransactionIds)`,
    /// but each transaction also includes the `miner_fee` and `legacy_sigop_count` fields.
    //
    // TODO: make the Transactions response return VerifiedUnminedTx,
    //       and remove the FullTransactions variant
    FullTransactions,

    /// Query matching cached rejected transaction IDs in the mempool,
    /// using a unique set of [`UnminedTxId`]s.
    RejectedTransactionIds(HashSet<UnminedTxId>),

    /// Queue a list of gossiped transactions or transaction IDs.
    ///
    /// The transaction downloader checks for duplicates across IDs and transactions.
    Queue(Vec<Gossip>),

    /// Queue a list of gossiped transactions or transaction IDs from the crawler.
    QueueFromCrawler(Vec<Gossip>),

    /// Queue transactions or transaction IDs received from a specific peer,
    /// tagging each one with the peer so the downloader can enforce a per-peer
    /// queue cap. See `GHSA-4fc2-h7jh-287c`.
    QueueFromPeer {
        /// The gossiped transaction candidates received from the peer.
        transactions: Vec<Gossip>,
        /// The peer that advertised them.
        source: QueueSource,
    },

    /// Queue a full transaction from the feature-gated private local path.
    #[cfg(feature = "privacy-admission")]
    QueuePrivate {
        /// The full transaction contents, never an ID-only gossip announcement.
        transaction: UnminedTx,
        /// The private-admission metadata for the submission.
        context: AdmissionContext,
    },

    /// Query aggregate-only private-pool diagnostics.
    #[cfg(feature = "privacy-admission")]
    PrivatePoolDiagnostics,

    /// Synchronously promote the complete private batch that is currently due.
    #[cfg(feature = "privacy-admission")]
    PromotePrivateDue,

    /// Check for newly verified transactions.
    ///
    /// The transaction downloader does not push transactions into the mempool.
    /// So a task should send this request regularly (every 5-10 seconds).
    ///
    /// These checks also happen for other request variants,
    /// but we can't rely on peers to send queries regularly,
    /// and crawler queue requests depend on peer responses.
    /// Also, crawler requests aren't frequent enough for transaction propagation.
    ///
    /// # Correctness
    ///
    /// This request is required to avoid hangs in the mempool.
    ///
    /// The queue checker task can't call `poll_ready` directly on the mempool
    /// service, because the service is wrapped in a `Buffer`. Calling
    /// `Buffer::poll_ready` reserves a buffer slot, which can cause hangs
    /// when too many slots are reserved but unused:
    /// <https://docs.rs/tower/0.4.10/tower/buffer/struct.Buffer.html#a-note-on-choosing-a-bound>
    CheckForVerifiedTransactions,

    /// Request summary statistics from the mempool for `getmempoolinfo`.
    QueueStats,

    /// Check whether a transparent output is spent in the mempool.
    UnspentOutput(transparent::OutPoint),
}

/// A response to a mempool service request.
///
/// Responses can read the current set of mempool transactions,
/// check the queued status of transactions to be downloaded and verified, or
/// confirm that the mempool has been checked for newly verified transactions.
#[derive(Debug)]
pub enum Response {
    /// Returns all [`UnminedTxId`]s from the mempool.
    TransactionIds(HashSet<UnminedTxId>),

    /// Returns matching [`UnminedTx`] from the mempool.
    ///
    /// Since the [`Request::TransactionsById`] request is unique,
    /// the response transactions are also unique. The same applies to
    /// [`Request::TransactionsByMinedId`] requests, since the mempool does not allow
    /// different transactions with different mined IDs.
    Transactions(Vec<UnminedTx>),

    /// Response to [`Request::AwaitOutput`] with the transparent output
    UnspentOutput(transparent::Output),

    /// Response to [`Request::TransactionWithDepsByMinedId`].
    TransactionWithDeps {
        /// The queried transaction
        transaction: VerifiedUnminedTx,
        /// A list of dependencies of the queried transaction.
        dependencies: HashSet<transaction::Hash>,
    },

    /// Returns all [`VerifiedUnminedTx`] in the mempool.
    //
    // TODO: make the Transactions response return VerifiedUnminedTx,
    //       and remove the FullTransactions variant
    FullTransactions {
        /// All [`VerifiedUnminedTx`]s in the mempool
        transactions: Vec<VerifiedUnminedTx>,

        /// All transaction dependencies in the mempool
        transaction_dependencies: TransactionDependencies,

        /// Last seen chain tip hash by mempool service
        last_seen_tip_hash: zakura_chain::block::Hash,
    },

    /// Returns matching cached rejected [`UnminedTxId`]s from the mempool,
    RejectedTransactionIds(HashSet<UnminedTxId>),

    /// Returns a list of initial queue checks results and a oneshot receiver
    /// for awaiting download and/or verification results.
    ///
    /// Each result matches the request at the corresponding vector index.
    Queued(Vec<Result<oneshot::Receiver<Result<(), BoxError>>, BoxError>>),

    /// Reports private admission status and optional verification completion.
    #[cfg(feature = "privacy-admission")]
    PrivateQueued {
        /// Whether this request created a reservation or already existed.
        status: PrivateAdmissionStatus,
        /// Completion for a newly accepted reservation; absent for an exact retry.
        completion: Option<oneshot::Receiver<Result<(), BoxError>>>,
    },

    /// Aggregate-only private-pool diagnostics.
    #[cfg(feature = "privacy-admission")]
    PrivatePoolDiagnostics(PrivatePoolDiagnostics),

    /// Aggregate result of a synchronous private promotion attempt.
    #[cfg(feature = "privacy-admission")]
    PrivatePromoted(PrivatePromotionOutcome),

    /// Confirms that the mempool has checked for recently verified transactions.
    CheckedForVerifiedTransactions,

    /// Summary statistics for the mempool: count, total size, memory usage, and regtest info.
    QueueStats {
        /// Number of transactions currently in the mempool
        size: usize,
        /// Total size in bytes of all transactions
        bytes: usize,
        /// Estimated memory usage in bytes
        usage: usize,
        /// Whether all transactions have been fully notified (regtest only)
        fully_notified: Option<bool>,
    },

    /// Returns whether a transparent output is created or spent in the mempool, if present.
    TransparentOutput(Option<CreatedOrSpent>),
}

/// Indicates whether an output was created or spent by a mempool transaction.
#[derive(Debug)]
pub enum CreatedOrSpent {
    /// An unspent output that was created by a transaction in the mempool and not spent by any other mempool tx.
    Created {
        /// The output
        output: transparent::Output,
        /// The version
        tx_version: u32,
        /// The last seen hash
        last_seen_hash: block::Hash,
    },
    /// Indicates that an output was spent by a mempool transaction.
    Spent,
}
