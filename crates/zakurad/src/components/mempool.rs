//! Zakura mempool.
//!
//! A zakurad application component that manages the active collection, reception,
//! gossip, verification, in-memory storage, eviction, and rejection of unmined Zcash
//! transactions (those that have not been confirmed in a mined block on the
//! blockchain).
//!
//! Major parts of the mempool include:
//!  * [Mempool Service][`Mempool`]
//!    * activates when the syncer is near the chain tip
//!    * spawns [download and verify tasks][`downloads::Downloads`] for each crawled or gossiped transaction
//!    * handles in-memory [storage][`storage::Storage`] of unmined transactions
//!  * [Crawler][`crawler::Crawler`]
//!    * runs in the background to periodically poll peers for fresh unmined transactions
//!  * [Queue Checker][`queue_checker::QueueChecker`]
//!    * runs in the background, polling the mempool to store newly verified transactions
//!  * [Transaction Gossip Task][`gossip::run_mempool_transaction_id_gossip`]
//!    * runs in the background and gossips newly added mempool transactions
//!      to peers

use std::{
    collections::HashSet,
    fmt,
    future::Future,
    iter,
    pin::{pin, Pin},
    task::{Context, Poll},
};

#[cfg(feature = "privacy-admission")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::{future::FutureExt, stream::Stream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tower::{buffer::Buffer, timeout::Timeout, util::BoxService, Service};

use zakura_chain::{
    block::{self, Height},
    chain_sync_status::ChainSyncStatus,
    chain_tip::ChainTip,
    transaction::UnminedTxId,
};
#[cfg(feature = "privacy-admission")]
use zakura_consensus::VERIFIER_BUFFER_BOUND;
use zakura_consensus::{error::TransactionError, transaction};
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::mempool::Gossip;
use zakura_node_services::mempool::{
    AdmissionOrigin, CreatedOrSpent, MempoolChange, MempoolDisabledError, MempoolTxSubscriber,
    QueueSource, Request, Response,
};
use zakura_state as zs;
use zakura_state::{ChainTipChange, TipAction};

use crate::components::sync::SyncStatus;

pub mod config;
mod crawler;
pub mod downloads;
mod error;
pub mod gossip;
mod pending_outputs;
#[cfg(feature = "privacy-admission")]
pub mod private_pool;
#[cfg(feature = "privacy-admission")]
mod private_release_scheduler;
#[cfg(feature = "privacy-admission")]
mod private_state;
mod queue_checker;
mod storage;

#[cfg(test)]
mod tests;

pub use crate::BoxError;

pub use config::Config;
pub use crawler::Crawler;
pub use error::MempoolError;
pub(crate) use gossip::run_mempool_transaction_id_gossip;
#[cfg(feature = "privacy-admission")]
pub(crate) use private_release_scheduler::PrivateReleaseScheduler;
pub use queue_checker::QueueChecker;
pub use storage::{
    ExactTipRejectionError, SameEffectsChainRejectionError, SameEffectsTipRejectionError, Storage,
};

#[cfg(feature = "privacy-admission")]
struct PrivateLifecycle {
    operations_open: Arc<AtomicBool>,
}

#[cfg(feature = "privacy-admission")]
impl PrivateLifecycle {
    fn new() -> Self {
        Self {
            operations_open: Arc::new(AtomicBool::new(true)),
        }
    }

    fn control(&self) -> PrivateLifecycleControl {
        PrivateLifecycleControl {
            operations_open: Arc::clone(&self.operations_open),
        }
    }

    fn accepts_operations(&self) -> bool {
        self.operations_open.load(Ordering::Acquire)
    }
}

#[cfg(feature = "privacy-admission")]
pub(crate) struct PrivateLifecycleControl {
    operations_open: Arc<AtomicBool>,
}

#[cfg(feature = "privacy-admission")]
impl PrivateLifecycleControl {
    pub(crate) fn close(&self) {
        self.operations_open.store(false, Ordering::Release);
    }
}

/// Canonical chain tip used for contextual mempool verification.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct VerificationTip {
    hash_and_height: Option<(block::Hash, Height)>,
}

impl VerificationTip {
    /// Construct verification metadata from a canonical block hash and height.
    pub const fn new(hash_and_height: Option<(block::Hash, Height)>) -> Self {
        Self { hash_and_height }
    }

    /// Return the canonical block hash and height.
    pub const fn hash_and_height(self) -> Option<(block::Hash, Height)> {
        self.hash_and_height
    }

    /// Return the canonical block height.
    pub const fn height(self) -> Option<Height> {
        match self.hash_and_height {
            Some((_hash, height)) => Some(height),
            None => None,
        }
    }
}

impl fmt::Debug for VerificationTip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerificationTip(private)")
    }
}

#[cfg(feature = "privacy-admission")]
use private_state::{PrivateAdmissionState, PrivateReservationOutcome};

#[cfg(test)]
pub use self::tests::UnboxMempoolError;

use downloads::{
    Downloads as TxDownloads, PendingTransaction, TransactionDownloadVerifyError,
    TRANSACTION_DOWNLOAD_TIMEOUT, TRANSACTION_VERIFY_TIMEOUT,
};

fn legacy_peer_log_label(addr: PeerSocketAddr, expose_peer_addresses: bool) -> String {
    if expose_peer_addresses {
        format!("legacy:{}", addr.remove_socket_addr_privacy())
    } else {
        "legacy:redacted".to_string()
    }
}

fn queue_source_log_label(source: &QueueSource, expose_peer_addresses: bool) -> String {
    match source {
        QueueSource::LegacySocket(addr) => {
            legacy_peer_log_label(PeerSocketAddr::from(*addr), expose_peer_addresses)
        }
        QueueSource::Zakura(peer_id) => format!("zakura:{peer_id:?}"),
    }
}

fn transaction_error_peer_log_label(
    error: &TransactionDownloadVerifyError,
    expose_peer_addresses: bool,
) -> Option<String> {
    match error {
        TransactionDownloadVerifyError::Invalid {
            advertiser_addr: Some(addr),
            ..
        } => Some(legacy_peer_log_label(*addr, expose_peer_addresses)),
        _ => None,
    }
}

type Outbound = Buffer<BoxService<zn::Request, zn::Response, zn::BoxError>, zn::Request>;
type State = Buffer<BoxService<zs::Request, zs::Response, zs::BoxError>, zs::Request>;
type TxVerifier = Buffer<
    BoxService<transaction::Request, transaction::Response, TransactionError>,
    transaction::Request,
>;
type InboundTxDownloads = TxDownloads<Timeout<Outbound>, Timeout<TxVerifier>, State>;

struct QueueAdmission {
    transactions: Vec<Gossip>,
    origin: AdmissionOrigin,
}

fn queue_admission(
    storage: &mut Storage,
    tx_downloads: &mut Pin<Box<InboundTxDownloads>>,
    admission: QueueAdmission,
) -> Vec<Result<oneshot::Receiver<Result<(), BoxError>>, BoxError>> {
    admission
        .transactions
        .into_iter()
        .map(
            |gossiped_tx| -> Result<oneshot::Receiver<Result<(), BoxError>>, MempoolError> {
                let (rsp_tx, rsp_rx) = oneshot::channel();
                match storage.should_download_or_verify(gossiped_tx.id()) {
                    Ok(()) => {}
                    Err(
                        error @ MempoolError::StorageExactTip(
                            ExactTipRejectionError::FailedStandard(
                                storage::NonStandardTransactionError::TransactionTooLarge {
                                    ..
                                },
                            ),
                        ),
                    ) => {
                        let _ = rsp_tx.send(Err(error.into()));
                        return Ok(rsp_rx);
                    }
                    Err(error) => return Err(error),
                }
                tx_downloads.download_if_needed_and_verify(
                    PendingTransaction::new(gossiped_tx, admission.origin.clone()),
                    Some(rsp_tx),
                )?;

                Ok(rsp_rx)
            },
        )
        .map(|result| result.map_err(BoxError::from))
        .collect()
}

fn transaction_misbehavior(
    error: &TransactionDownloadVerifyError,
) -> Option<(PeerSocketAddr, u32)> {
    let TransactionDownloadVerifyError::Invalid {
        error,
        advertiser_addr: Some(advertiser_addr),
    } = error
    else {
        return None;
    };

    let score = error.mempool_misbehavior_score();
    (score != 0).then_some((*advertiser_addr, score))
}

/// The state of the mempool.
///
/// Indicates whether it is enabled or disabled and, if enabled, contains
/// the necessary data to run it.
//
// Zakura only has one mempool, so the enum variant size difference doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Default)]
enum ActiveState {
    /// The Mempool is disabled.
    #[default]
    Disabled,

    /// The Mempool is enabled.
    Enabled {
        /// The Mempool storage itself.
        ///
        /// # Correctness
        ///
        /// Only components internal to the [`Mempool`] struct are allowed to
        /// inject transactions into `storage`, as transactions must be verified beforehand.
        storage: Storage,

        /// The transaction download and verify stream.
        tx_downloads: Pin<Box<InboundTxDownloads>>,

        #[cfg(feature = "privacy-admission")]
        /// The private transaction download and verify stream.
        private_tx_downloads: Pin<Box<InboundTxDownloads>>,

        /// Verified transaction IDs awaiting proactive advertisement through
        /// the peer set.
        ///
        /// This pending set is separate from the full mempool inventory served
        /// by [`Request::TransactionIds`] when a peer requests it.
        pending_gossip_tx_ids: HashSet<UnminedTxId>,

        /// Last seen chain tip hash that mempool transactions have been verified against.
        ///
        /// In some tests, this is initialized to the latest chain tip, then updated in `poll_ready()` before each request.
        last_seen_tip_hash: block::Hash,
    },
}

impl ActiveState {
    /// Returns the current state, leaving [`Self::Disabled`] in its place.
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Returns a list of requests that will retry every stored and pending transaction.
    fn transaction_retry_requests(&self) -> Vec<PendingTransaction> {
        match self {
            ActiveState::Disabled => Vec::new(),
            ActiveState::Enabled {
                storage,
                tx_downloads,
                ..
            } => {
                let mut transactions = Vec::new();

                let storage = storage.transactions().values().map(|tx| {
                    let origin = AdmissionOrigin::LegacyLocal;
                    PendingTransaction::new(tx.transaction.clone().into(), origin)
                });
                transactions.extend(storage);

                let pending = tx_downloads
                    .transaction_requests()
                    .map(PendingTransaction::retry);
                transactions.extend(pending);

                transactions
            }
        }
    }

    #[cfg(feature = "privacy-admission")]
    fn private_transaction_retry_requests(&self) -> Vec<PendingTransaction> {
        match self {
            ActiveState::Disabled => Vec::new(),
            ActiveState::Enabled {
                private_tx_downloads,
                ..
            } => private_tx_downloads
                .transaction_requests()
                .map(PendingTransaction::retry)
                .collect(),
        }
    }

    /// Returns the number of pending transactions waiting for download or verify,
    /// or zero if the mempool is disabled.
    #[cfg(feature = "progress-bar")]
    fn queued_transaction_count(&self) -> usize {
        match self {
            ActiveState::Disabled => 0,
            ActiveState::Enabled { tx_downloads, .. } => tx_downloads.in_flight(),
        }
    }

    /// Returns the number of transactions in storage, or zero if the mempool is disabled.
    #[cfg(feature = "progress-bar")]
    fn transaction_count(&self) -> usize {
        match self {
            ActiveState::Disabled => 0,
            ActiveState::Enabled { storage, .. } => storage.transaction_count(),
        }
    }

    /// Returns the cost of the transactions in the mempool, according to ZIP-401.
    /// Returns zero if the mempool is disabled.
    #[cfg(feature = "progress-bar")]
    fn total_cost(&self) -> u64 {
        match self {
            ActiveState::Disabled => 0,
            ActiveState::Enabled { storage, .. } => storage.total_cost(),
        }
    }

    /// Returns the total serialized size of the verified transactions in the set,
    /// or zero if the mempool is disabled.
    ///
    /// See [`Storage::total_serialized_size()`] for details.
    #[cfg(feature = "progress-bar")]
    pub fn total_serialized_size(&self) -> usize {
        match self {
            ActiveState::Disabled => 0,
            ActiveState::Enabled { storage, .. } => storage.total_serialized_size(),
        }
    }

    /// Returns the number of rejected transaction hashes in storage,
    /// or zero if the mempool is disabled.
    #[cfg(feature = "progress-bar")]
    fn rejected_transaction_count(&mut self) -> usize {
        match self {
            ActiveState::Disabled => 0,
            ActiveState::Enabled { storage, .. } => storage.rejected_transaction_count(),
        }
    }
}

/// Mempool async management and query service.
///
/// The mempool is the set of all verified transactions that this node is aware
/// of that have yet to be confirmed by the Zcash network. A transaction is
/// confirmed when it has been included in a block ('mined').
pub struct Mempool {
    /// The configurable options for the mempool, persisted between states.
    config: Config,

    /// Whether legacy peer address labels in logs are unredacted.
    expose_peer_addresses: bool,

    /// The state of the mempool.
    active_state: ActiveState,

    #[cfg(feature = "privacy-admission")]
    private_admission: PrivateAdmissionState,

    #[cfg(feature = "privacy-admission")]
    private_lifecycle: PrivateLifecycle,

    /// Allows checking if we are near the tip to enable/disable the mempool.
    sync_status: SyncStatus,

    /// If the state's best chain tip has reached this height, always enable the mempool.
    debug_enable_at_height: Option<Height>,

    /// Allows efficient access to the best tip of the blockchain.
    latest_chain_tip: zs::LatestChainTip,

    /// Allows the detection of newly added chain tip blocks,
    /// and chain tip resets.
    chain_tip_change: ChainTipChange,

    /// Handle to the outbound service.
    /// Used to construct the transaction downloader.
    outbound: Outbound,

    /// Handle to the state service.
    /// Used to construct the transaction downloader.
    state: State,

    /// Handle to the transaction verifier service.
    /// Used to construct the transaction downloader.
    tx_verifier: TxVerifier,

    /// Sender part of a gossip transactions channel.
    /// Used to broadcast transaction ids to peers.
    transaction_sender: broadcast::Sender<MempoolChange>,

    /// Sender for reporting peer addresses that advertised unexpectedly invalid transactions.
    misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,

    // Diagnostics
    //
    /// Queued transactions pending download or verification transmitter.
    /// Only displayed after the mempool's first activation.
    #[cfg(feature = "progress-bar")]
    queued_count_bar: Option<howudoin::Tx>,

    /// Number of mempool transactions transmitter.
    /// Only displayed after the mempool's first activation.
    #[cfg(feature = "progress-bar")]
    transaction_count_bar: Option<howudoin::Tx>,

    /// Mempool transaction cost transmitter.
    /// Only displayed after the mempool's first activation.
    #[cfg(feature = "progress-bar")]
    transaction_cost_bar: Option<howudoin::Tx>,

    /// Rejected transactions transmitter.
    /// Only displayed after the mempool's first activation.
    #[cfg(feature = "progress-bar")]
    rejected_count_bar: Option<howudoin::Tx>,
}

impl Mempool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
        expose_peer_addresses: bool,
        outbound: Outbound,
        state: State,
        tx_verifier: TxVerifier,
        sync_status: SyncStatus,
        latest_chain_tip: zs::LatestChainTip,
        chain_tip_change: ChainTipChange,
        misbehavior_sender: mpsc::Sender<(PeerSocketAddr, u32)>,
    ) -> (Self, MempoolTxSubscriber) {
        let (transaction_sender, _) =
            tokio::sync::broadcast::channel(gossip::MEMPOOL_CHANGE_CHANNEL_CAPACITY);
        let transaction_subscriber = MempoolTxSubscriber::new(transaction_sender.clone());

        let mut service = Mempool {
            config: config.clone(),
            expose_peer_addresses,
            active_state: ActiveState::Disabled,
            #[cfg(feature = "privacy-admission")]
            private_admission: PrivateAdmissionState::new(config.private_pool),
            #[cfg(feature = "privacy-admission")]
            private_lifecycle: PrivateLifecycle::new(),
            sync_status,
            debug_enable_at_height: config.debug_enable_at_height.map(Height),
            latest_chain_tip,
            chain_tip_change,
            outbound,
            state,
            tx_verifier,
            transaction_sender,
            misbehavior_sender,
            #[cfg(feature = "progress-bar")]
            queued_count_bar: None,
            #[cfg(feature = "progress-bar")]
            transaction_count_bar: None,
            #[cfg(feature = "progress-bar")]
            transaction_cost_bar: None,
            #[cfg(feature = "progress-bar")]
            rejected_count_bar: None,
        };

        // Make sure `is_enabled` is accurate.
        // Otherwise, it is only updated in `poll_ready`, right before each service call.
        service.update_state(None);

        (service, transaction_subscriber)
    }

    #[cfg(feature = "privacy-admission")]
    pub(crate) fn set_private_scheduler_state(
        &mut self,
        state: tokio::sync::watch::Receiver<zakura_node_services::mempool::SchedulerState>,
    ) {
        self.private_admission.set_scheduler_state(state);
    }

    #[cfg(feature = "privacy-admission")]
    pub(crate) fn private_release_timing(&self) -> private_release_scheduler::PrivateReleaseTiming {
        self.private_admission.release_timing()
    }

    #[cfg(feature = "privacy-admission")]
    pub(crate) fn private_lifecycle_control(&self) -> PrivateLifecycleControl {
        self.private_lifecycle.control()
    }

    /// Is the mempool enabled by a debug config option?
    fn is_enabled_by_debug(&self) -> bool {
        let mut is_debug_enabled = false;

        // optimise non-debug performance
        if self.debug_enable_at_height.is_none() {
            return is_debug_enabled;
        }

        let enable_at_height = self
            .debug_enable_at_height
            .expect("unexpected debug_enable_at_height: just checked for None");

        if let Some(best_tip_height) = self.latest_chain_tip.best_tip_height() {
            is_debug_enabled = best_tip_height >= enable_at_height;

            if is_debug_enabled && !self.is_enabled() {
                info!(
                    ?best_tip_height,
                    ?enable_at_height,
                    "enabling mempool for debugging"
                );
            }
        }

        is_debug_enabled
    }

    /// Returns `true` if Zakura is caught up enough to start the mempool.
    ///
    /// During Zakura sync, [`SyncStatus`] is updated from state's effective
    /// best-header frontier. That frontier uses the verified block tip when it
    /// is ahead of the stored header tip, so a locally mined latest block does
    /// not keep the mempool disabled just because peers have not advertised its
    /// header yet.
    fn is_caught_up_to_start(&self) -> bool {
        self.sync_status.is_close_to_tip() || self.is_enabled_by_debug()
    }

    /// Replaces the active state with a freshly-initialised [`ActiveState::Enabled`],
    /// using `tip_action`'s best tip hash as the `last_seen_tip_hash`.
    fn enable_at_tip(&mut self, tip_action: &TipAction) {
        let (last_seen_tip_hash, tip_height) = tip_action.best_tip_hash_and_height();

        info!(
            ?tip_height,
            "activating mempool: Zakura is close to the tip"
        );

        let tx_downloads = Box::pin(TxDownloads::new(
            Timeout::new(self.outbound.clone(), TRANSACTION_DOWNLOAD_TIMEOUT),
            Timeout::new(self.tx_verifier.clone(), TRANSACTION_VERIFY_TIMEOUT),
            self.state.clone(),
            self.expose_peer_addresses,
            self.config.max_transaction_bytes,
        ));
        #[cfg(feature = "privacy-admission")]
        let private_tx_downloads = Box::pin(
            TxDownloads::new(
                Timeout::new(self.outbound.clone(), TRANSACTION_DOWNLOAD_TIMEOUT),
                Timeout::new(self.tx_verifier.clone(), TRANSACTION_VERIFY_TIMEOUT),
                self.state.clone(),
                self.expose_peer_addresses,
                self.config.max_transaction_bytes,
            )
            .with_capacity(
                self.config
                    .private_pool
                    .max_transactions()
                    .min(VERIFIER_BUFFER_BOUND - 1),
            ),
        );
        self.active_state = ActiveState::Enabled {
            storage: storage::Storage::new(&self.config),
            tx_downloads,
            #[cfg(feature = "privacy-admission")]
            private_tx_downloads,
            pending_gossip_tx_ids: HashSet::new(),
            last_seen_tip_hash,
        };
    }

    /// Update the mempool state (enabled / disabled) depending on how close to
    /// the tip is the synchronization, including side effects to state changes.
    ///
    /// Accepts an optional [`TipAction`] for setting the `last_seen_tip_hash` field
    /// when enabling the mempool state, it will not enable the mempool if this is None.
    ///
    /// Returns `true` if the state changed.
    fn update_state(&mut self, tip_action: Option<&TipAction>) -> bool {
        let is_caught_up_to_start = self.is_caught_up_to_start();

        // TODO: revisit these state transitions after header sync can prove
        // whether Zakura is behind the network tip.
        match (is_caught_up_to_start, self.is_enabled(), tip_action) {
            // the active state is up to date, or there is no tip action to activate the mempool
            (false, false, _) | (true, true, _) | (true, false, None) => return false,

            // Enable state - there should be a chain tip when Zakura is close
            // to the network tip.
            (true, false, Some(tip_action)) => self.enable_at_tip(tip_action),

            // TODO: only disable an already-active mempool when a validated
            // Zakura header/block-sync frontier proves Zakura is behind a
            // higher-work chain that follows this node's consensus rules.
            //
            // The legacy sync status can be triggered by lower-work forks,
            // stale peers, or peers on incompatible consensus rules, so
            // it is strong enough to delay initial activation but not to shut
            // down a working mempool.
            (false, true, _) => {
                return false;
            }
        };

        true
    }

    /// Return whether the mempool is enabled or not.
    pub fn is_enabled(&self) -> bool {
        match self.active_state {
            ActiveState::Disabled => false,
            ActiveState::Enabled { .. } => true,
        }
    }

    /// Update metrics for the mempool.
    fn update_metrics(&mut self) {
        // Shutdown if needed
        #[cfg(feature = "progress-bar")]
        if matches!(howudoin::cancelled(), Some(true)) {
            self.disable_metrics();
            return;
        }

        // Initialize if just activated
        #[cfg(feature = "progress-bar")]
        if self.is_enabled()
            && (self.queued_count_bar.is_none()
                || self.transaction_count_bar.is_none()
                || self.transaction_cost_bar.is_none()
                || self.rejected_count_bar.is_none())
        {
            let _max_transaction_count = self.config.tx_cost_limit
                / zakura_chain::transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD;

            let transaction_count_bar = *howudoin::new_root()
                .label("Mempool Transactions")
                .set_pos(0u64);
            // .set_len(max_transaction_count);

            let transaction_cost_bar = howudoin::new_with_parent(transaction_count_bar.id())
                .label("Mempool Cost")
                .set_pos(0u64)
                // .set_len(self.config.tx_cost_limit)
                .fmt_as_bytes(true);

            let queued_count_bar = *howudoin::new_with_parent(transaction_cost_bar.id())
                .label("Mempool Queue")
                .set_pos(0u64);
            // .set_len(
            //     u64::try_from(downloads::MAX_INBOUND_CONCURRENCY).expect("fits in u64"),
            // );

            let rejected_count_bar = *howudoin::new_with_parent(queued_count_bar.id())
                .label("Mempool Rejects")
                .set_pos(0u64);
            // .set_len(
            //     u64::try_from(storage::MAX_EVICTION_MEMORY_ENTRIES).expect("fits in u64"),
            // );

            self.transaction_count_bar = Some(transaction_count_bar);
            self.transaction_cost_bar = Some(transaction_cost_bar);
            self.queued_count_bar = Some(queued_count_bar);
            self.rejected_count_bar = Some(rejected_count_bar);
        }

        // Update if the mempool has ever been active
        #[cfg(feature = "progress-bar")]
        if let (
            Some(queued_count_bar),
            Some(transaction_count_bar),
            Some(transaction_cost_bar),
            Some(rejected_count_bar),
        ) = (
            self.queued_count_bar,
            self.transaction_count_bar,
            self.transaction_cost_bar,
            self.rejected_count_bar,
        ) {
            let queued_count = self.active_state.queued_transaction_count();
            let transaction_count = self.active_state.transaction_count();

            let transaction_cost = self.active_state.total_cost();
            let transaction_size = self.active_state.total_serialized_size();
            let transaction_size =
                indicatif::HumanBytes(transaction_size.try_into().expect("fits in u64"));

            let rejected_count = self.active_state.rejected_transaction_count();

            queued_count_bar.set_pos(u64::try_from(queued_count).expect("fits in u64"));

            transaction_count_bar.set_pos(u64::try_from(transaction_count).expect("fits in u64"));

            // Display the cost and cost limit, with the actual size as a description.
            //
            // Costs can be much higher than the transaction size due to the
            // MEMPOOL_TRANSACTION_COST_THRESHOLD minimum cost.
            transaction_cost_bar
                .set_pos(transaction_cost)
                .desc(format!("Actual size {transaction_size}"));

            rejected_count_bar.set_pos(u64::try_from(rejected_count).expect("fits in u64"));
        }
    }

    /// Disable metrics for the mempool.
    fn disable_metrics(&self) {
        #[cfg(feature = "progress-bar")]
        {
            if let Some(bar) = self.queued_count_bar {
                bar.close()
            }
            if let Some(bar) = self.transaction_count_bar {
                bar.close()
            }
            if let Some(bar) = self.transaction_cost_bar {
                bar.close()
            }
            if let Some(bar) = self.rejected_count_bar {
                bar.close()
            }
        }
    }
}

impl Service<Request> for Mempool {
    type Response = Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let should_check_tip = self.is_enabled() || self.is_caught_up_to_start();
        let tip_action = should_check_tip
            .then(|| self.chain_tip_change.last_tip_change())
            .flatten();

        // TODO: Consider broadcasting a `MempoolChange` when the mempool is disabled.
        let is_state_changed = self.update_state(tip_action.as_ref());

        tracing::trace!(is_enabled = ?self.is_enabled(), ?is_state_changed, "started polling the mempool...");

        // When the mempool is disabled we still return that the service is ready.
        // Otherwise, callers could block waiting for the mempool to be enabled.
        if !self.is_enabled() {
            self.update_metrics();

            return Poll::Ready(Ok(()));
        }

        // Clear the mempool and cancel downloads if there has been a chain tip reset.
        //
        // But if the mempool was just freshly enabled,
        // skip resetting and removing mined transactions for this tip.
        if !is_state_changed && matches!(tip_action, Some(TipAction::Reset { .. })) {
            info!(
                tip_height = ?tip_action.as_ref().unwrap().best_tip_height(),
                "resetting mempool: switched best chain, skipped blocks, or activated network upgrade"
            );

            #[cfg(feature = "privacy-admission")]
            let retained_retries = self
                .private_admission
                .begin_revalidation(
                    VerificationTip::new(
                        tip_action
                            .as_ref()
                            .map(|action| action.best_tip_hash_and_height()),
                    ),
                    true,
                )
                .into_iter()
                .map(|record| {
                    PendingTransaction::new(
                        Gossip::Tx(record.transaction().transaction.clone()),
                        AdmissionOrigin::PrivateLocal(record.context()),
                    )
                })
                .collect::<Vec<_>>();

            let previous_state = self.active_state.take();
            let tx_retries = previous_state.transaction_retry_requests();
            #[cfg(feature = "privacy-admission")]
            let private_tx_retries = previous_state.private_transaction_retry_requests();

            // Use the same code for dropping and resetting the mempool,
            // to avoid subtle bugs.
            //
            // Drop the current contents of the state,
            // cancelling any pending download tasks,
            // and dropping completed verification results.
            std::mem::drop(previous_state);

            // Re-initialise an empty state.
            //
            // This deliberately bypasses the initial-activation gate in `update_state()`:
            // the mempool was already active when the reset arrived, and the legacy
            // far-from-tip sync status must not disable an already-active mempool
            // (it can be triggered by lower-work forks, stale peers, or peers on
            // incompatible consensus rules).
            self.enable_at_tip(
                tip_action
                    .as_ref()
                    .expect("this branch only matches when tip_action is a Reset"),
            );

            // Re-verify the transactions that were pending or valid at the previous tip.
            // This saves us the time and data needed to re-download them.
            if let ActiveState::Enabled {
                tx_downloads,
                #[cfg(feature = "privacy-admission")]
                private_tx_downloads,
                ..
            } = &mut self.active_state
            {
                info!(
                    transactions = tx_retries.len(),
                    "re-verifying mempool transactions after a chain fork"
                );

                for tx in tx_retries {
                    // This is just an efficiency optimisation, so we don't care if queueing
                    // transaction requests fails.
                    let _result = tx_downloads.download_if_needed_and_verify(tx, None);
                }

                #[cfg(feature = "privacy-admission")]
                for tx in private_tx_retries.into_iter().chain(retained_retries) {
                    let transaction_id = tx.gossip().id();
                    let origin = tx.origin().clone();
                    let result = private_tx_downloads.download_if_needed_and_verify(tx, None);
                    if result.is_err() {
                        if let AdmissionOrigin::PrivateLocal(context) = origin {
                            self.private_admission
                                .revalidation_queue_failed(transaction_id, context);
                            self.private_admission.cancel_reservation(context);
                        }
                    }
                }
            }

            self.update_metrics();

            return Poll::Ready(Ok(()));
        }

        if let ActiveState::Enabled {
            storage,
            tx_downloads,
            #[cfg(feature = "privacy-admission")]
            private_tx_downloads,
            pending_gossip_tx_ids,
            last_seen_tip_hash,
        } = &mut self.active_state
        {
            // Collect inserted transaction ids.
            let mut send_to_peers_ids = HashSet::<_>::new();
            let mut invalidated_ids = HashSet::<_>::new();
            let mut mined_mempool_ids = HashSet::<_>::new();

            let current_tip = tip_action
                .as_ref()
                .map(|action| VerificationTip::new(Some(action.best_tip_hash_and_height())))
                .unwrap_or_else(|| {
                    VerificationTip::new(
                        self.latest_chain_tip
                            .best_tip_height_and_hash()
                            .map(|(height, hash)| (hash, height)),
                    )
                });
            let best_tip_height = current_tip.height();

            #[cfg(feature = "privacy-admission")]
            let download_streams = [&mut *tx_downloads, &mut *private_tx_downloads];
            #[cfg(not(feature = "privacy-admission"))]
            let download_streams = [&mut *tx_downloads];

            // Clean up completed download tasks and add to mempool if successful.
            for tx_downloads in download_streams {
                while let Poll::Ready(Some(result)) = pin!(&mut *tx_downloads).poll_next(cx) {
                    match result {
                        Ok(Ok((tx, spent_mempool_outpoints, verification_tip, rsp_tx, origin))) => {
                            if current_tip == verification_tip {
                                #[cfg(feature = "privacy-admission")]
                                if let AdmissionOrigin::PrivateLocal(context) = origin {
                                    self.private_admission.complete_verified(
                                        tx,
                                        spent_mempool_outpoints,
                                        verification_tip,
                                        context,
                                    );
                                    continue;
                                }
                                let tx_id = tx.transaction.id();
                                let (insert_result, evicted_ids) = storage.insert_with_evicted_ids(
                                    tx,
                                    spent_mempool_outpoints,
                                    best_tip_height,
                                );

                                tracing::trace!(
                                    ?insert_result,
                                    ?evicted_ids,
                                    "got Ok(_) transaction verify, tried to store",
                                );

                                if let Ok(inserted_id) = insert_result {
                                    // Save transaction ids that we will send to peers
                                    send_to_peers_ids.insert(inserted_id);
                                } else {
                                    invalidated_ids.insert(tx_id);
                                }

                                if !evicted_ids.is_empty() {
                                    // A later insertion can evict a transaction accepted earlier in
                                    // this `poll_ready` pass, so do not advertise it.
                                    send_to_peers_ids.retain(|id| !evicted_ids.contains(id));
                                    invalidated_ids.extend(evicted_ids);
                                }

                                // Send the result to responder channel if one was provided.
                                if let Some(rsp_tx) = rsp_tx {
                                    let _ = rsp_tx
                                        .send(insert_result.map(|_| ()).map_err(|err| err.into()));
                                }
                            } else {
                                tracing::trace!("chain grew during tx verification, retrying ..",);

                                // We don't care if re-queueing the transaction request fails.
                                #[cfg(feature = "privacy-admission")]
                                let transaction_id = tx.transaction.id();
                                #[cfg(feature = "privacy-admission")]
                                let result = tx_downloads.download_if_needed_and_verify(
                                    PendingTransaction::for_retry(tx.transaction, &origin),
                                    rsp_tx,
                                );
                                #[cfg(not(feature = "privacy-admission"))]
                                let _result = tx_downloads.download_if_needed_and_verify(
                                    PendingTransaction::for_retry(tx.transaction, &origin),
                                    rsp_tx,
                                );
                                #[cfg(feature = "privacy-admission")]
                                if result.is_err() {
                                    if let AdmissionOrigin::PrivateLocal(context) = origin {
                                        self.private_admission
                                            .revalidation_queue_failed(transaction_id, context);
                                    }
                                }
                            }
                        }
                        Ok(Err(boxed_err)) => {
                            let (tx_id, error, origin) = *boxed_err;
                            #[cfg(not(feature = "privacy-admission"))]
                            let _ = &origin;
                            #[cfg(feature = "privacy-admission")]
                            if let AdmissionOrigin::PrivateLocal(context) = origin {
                                self.private_admission.fail(tx_id, context, error);
                                continue;
                            }
                            if let Some((advertiser_addr, score)) = transaction_misbehavior(&error)
                            {
                                let _ = self.misbehavior_sender.try_send((advertiser_addr, score));
                            }

                            let peer_label = transaction_error_peer_log_label(
                                &error,
                                self.expose_peer_addresses,
                            );
                            let peer = peer_label.as_deref().unwrap_or("none");
                            tracing::debug!(
                                ?tx_id,
                                ?error,
                                peer = %peer,
                                "mempool transaction was not accepted"
                            );

                            match &error {
                                TransactionDownloadVerifyError::PolicyRejected(
                                    storage::NonStandardTransactionError::TransactionTooLarge {
                                        ..
                                    },
                                ) => metrics::counter!(
                                    "mempool.rejected.transactions.total",
                                    "reason" => "transaction_too_large"
                                )
                                .increment(1),
                                _ => metrics::counter!(
                                    "mempool.failed.verify.tasks.total",
                                    "reason" => error.to_string()
                                )
                                .increment(1),
                            }

                            invalidated_ids.insert(tx_id);
                            storage.reject_if_needed(tx_id, error);
                        }
                        Err((tx_id, _elapsed, origin)) => {
                            #[cfg(not(feature = "privacy-admission"))]
                            let _ = &origin;
                            #[cfg(feature = "privacy-admission")]
                            if let AdmissionOrigin::PrivateLocal(context) = origin {
                                self.private_admission
                                    .revalidation_timed_out(tx_id, context);
                                continue;
                            }
                            tracing::info!(
                                ?tx_id,
                                "mempool transaction failed to verify due to timeout"
                            );

                            invalidated_ids.insert(tx_id);

                            metrics::counter!("mempool.failed.verify.tasks.total", "reason" => "timeout").increment(1);
                        }
                    };
                }
            }

            // Handle best chain tip changes
            if let Some(TipAction::Grow { block }) = tip_action {
                tracing::trace!(block_height = ?block.height, "handling blocks added to tip");
                *last_seen_tip_hash = block.hash;

                // Cancel downloads/verifications/storage of transactions
                // with the same mined IDs as recently mined transactions.
                let mined_ids = block.transaction_hashes.iter().cloned().collect();
                #[cfg(feature = "privacy-admission")]
                self.private_admission.remove_mined(&mined_ids)?;
                tx_downloads.cancel(&mined_ids);
                #[cfg(feature = "privacy-admission")]
                private_tx_downloads.cancel(&mined_ids);
                storage.clear_mined_dependencies(&mined_ids);

                let storage::RemovedTransactionIds { mined, invalidated } =
                    storage.reject_and_remove_same_effects(&mined_ids, block.transactions);

                // Clear any transaction rejections if they might have become valid after
                // the new block was added to the tip.
                storage.clear_tip_rejections();

                mined_mempool_ids.extend(mined);
                invalidated_ids.extend(invalidated);

                #[cfg(feature = "privacy-admission")]
                for record in self
                    .private_admission
                    .begin_revalidation(current_tip, false)
                {
                    let transaction_id = record.transaction_id();
                    let context = record.context();
                    let result = private_tx_downloads.download_if_needed_and_verify(
                        PendingTransaction::new(
                            Gossip::Tx(record.transaction().transaction.clone()),
                            AdmissionOrigin::PrivateLocal(context),
                        ),
                        None,
                    );
                    if result.is_err() {
                        self.private_admission
                            .revalidation_queue_failed(transaction_id, context);
                    }
                }
            }

            // Remove expired transactions from the mempool.
            //
            // Lock times never expire, because block times are strictly increasing.
            // So we don't need to check them here.
            if let Some(tip_height) = best_tip_height {
                let expired_transactions = storage.remove_expired_transactions(tip_height);
                // Remove transactions that are expired from the peers list
                send_to_peers_ids.retain(|id| !expired_transactions.contains(id));

                if !expired_transactions.is_empty() {
                    tracing::debug!(
                        ?expired_transactions,
                        "removed expired transactions from the mempool",
                    );

                    invalidated_ids.extend(expired_transactions);
                }
            }

            // Send transactions that were not rejected nor expired to peers and RPC listeners.
            if !send_to_peers_ids.is_empty() {
                tracing::trace!(
                    ?send_to_peers_ids,
                    "sending new transactions to peers and RPC listeners"
                );

                pending_gossip_tx_ids.extend(send_to_peers_ids.iter().copied());

                self.transaction_sender
                    .send(MempoolChange::added(send_to_peers_ids))?;
            }

            // Prune transaction IDs that no longer need gossip from the pending set.
            //
            // This runs after the pending set is extended, so it also covers
            // transactions that were inserted and then invalidated or mined
            // within this same `poll_ready` call.
            if !invalidated_ids.is_empty() || !mined_mempool_ids.is_empty() {
                pending_gossip_tx_ids.retain(|tx_id| {
                    !invalidated_ids.contains(tx_id) && !mined_mempool_ids.contains(tx_id)
                });
            }

            // Send transactions that were rejected to RPC listeners.
            if !invalidated_ids.is_empty() {
                tracing::trace!(
                    ?invalidated_ids,
                    "sending invalidated transactions to RPC listeners"
                );

                self.transaction_sender
                    .send(MempoolChange::invalidated(invalidated_ids))?;
            }

            // Send transactions that were mined onto the best chain to RPC listeners.
            if !mined_mempool_ids.is_empty() {
                tracing::trace!(
                    ?mined_mempool_ids,
                    "sending mined transactions to RPC listeners"
                );

                self.transaction_sender
                    .send(MempoolChange::mined(mined_mempool_ids))?;
            }
        }

        self.update_metrics();

        Poll::Ready(Ok(()))
    }

    /// Call the mempool service.
    ///
    /// Errors indicate that the peer has done something wrong or unexpected,
    /// and will cause callers to disconnect from the remote peer.
    #[instrument(name = "mempool", skip(self, req))]
    fn call(&mut self, req: Request) -> Self::Future {
        #[cfg(feature = "privacy-admission")]
        if matches!(
            &req,
            Request::QueuePrivate { .. } | Request::PromotePrivateDue
        ) && !self.private_lifecycle.accepts_operations()
        {
            return async move { Err(MempoolError::PrivateOperationsClosed.into()) }.boxed();
        }

        match &mut self.active_state {
            ActiveState::Enabled {
                storage,
                tx_downloads,
                #[cfg(feature = "privacy-admission")]
                private_tx_downloads,
                pending_gossip_tx_ids,
                last_seen_tip_hash,
            } => match req {
                // Queries
                Request::TransactionIds => {
                    trace!(?req, "got mempool request");

                    let res: HashSet<_> = storage.tx_ids().collect();

                    trace!(?req, res_count = ?res.len(), "answered mempool request");

                    async move { Ok(Response::TransactionIds(res)) }.boxed()
                }

                Request::TakePendingGossipTransactionIds { limit } => {
                    trace!(?req, "got mempool request");

                    let res = if pending_gossip_tx_ids.len() <= limit {
                        std::mem::take(pending_gossip_tx_ids)
                    } else {
                        let res: HashSet<_> =
                            pending_gossip_tx_ids.iter().copied().take(limit).collect();

                        for tx_id in &res {
                            pending_gossip_tx_ids.remove(tx_id);
                        }

                        res
                    };

                    trace!(?req, res_count = ?res.len(), "answered mempool request");

                    async move { Ok(Response::TransactionIds(res)) }.boxed()
                }

                Request::TransactionsById(ref ids) => {
                    trace!(?req, "got mempool request");

                    let res: Vec<_> = storage.transactions_exact(ids.clone()).cloned().collect();

                    trace!(?req, res_count = ?res.len(), "answered mempool request");

                    async move { Ok(Response::Transactions(res)) }.boxed()
                }
                Request::TransactionsByMinedId(ref ids) => {
                    trace!(?req, "got mempool request");

                    let res: Vec<_> = storage
                        .transactions_same_effects(ids.clone())
                        .cloned()
                        .collect();

                    trace!(?req, res_count = ?res.len(), "answered mempool request");

                    async move { Ok(Response::Transactions(res)) }.boxed()
                }
                Request::TransactionWithDepsByMinedId(tx_id) => {
                    trace!(?req, "got mempool request");

                    let res = if let Some((transaction, dependencies)) =
                        storage.transaction_with_deps(tx_id)
                    {
                        Ok(Response::TransactionWithDeps {
                            transaction,
                            dependencies,
                        })
                    } else {
                        Err("transaction not found in mempool".into())
                    };

                    trace!(?req, ?res, "answered mempool request");

                    async move { res }.boxed()
                }

                Request::AwaitOutput(outpoint) => {
                    trace!(?req, "got mempool request");

                    let response_fut = storage.pending_outputs.queue(outpoint);

                    if let Some(output) = storage.created_output(&outpoint) {
                        storage.pending_outputs.respond(&outpoint, output)
                    }

                    trace!("answered mempool request");

                    response_fut.boxed()
                }

                Request::FullTransactions => {
                    trace!(?req, "got mempool request");

                    let transactions: Vec<_> = storage.transactions().values().cloned().collect();
                    let transaction_dependencies = storage.transaction_dependencies().clone();

                    trace!(?req, transactions_count = ?transactions.len(), "answered mempool request");

                    let response = Response::FullTransactions {
                        transactions,
                        transaction_dependencies,
                        last_seen_tip_hash: *last_seen_tip_hash,
                    };

                    async move { Ok(response) }.boxed()
                }

                Request::RejectedTransactionIds(ref ids) => {
                    trace!(?req, "got mempool request");

                    let res = storage.rejected_transactions(ids.clone()).collect();

                    trace!(?req, ?res, "answered mempool request");

                    async move { Ok(Response::RejectedTransactionIds(res)) }.boxed()
                }

                // Queue mempool candidates
                Request::Queue(gossiped_txs) => {
                    trace!(req_count = ?gossiped_txs.len(), "got mempool Queue request");
                    let rsp = queue_admission(
                        storage,
                        tx_downloads,
                        QueueAdmission {
                            transactions: gossiped_txs,
                            origin: AdmissionOrigin::LegacyLocal,
                        },
                    );

                    // We've added transactions to the queue
                    self.update_metrics();

                    async move { Ok(Response::Queued(rsp)) }.boxed()
                }

                Request::QueueFromCrawler(gossiped_txs) => {
                    trace!(req_count = ?gossiped_txs.len(), "got mempool QueueFromCrawler request");
                    let rsp = queue_admission(
                        storage,
                        tx_downloads,
                        QueueAdmission {
                            transactions: gossiped_txs,
                            origin: AdmissionOrigin::Crawler,
                        },
                    );

                    self.update_metrics();
                    async move { Ok(Response::Queued(rsp)) }.boxed()
                }

                // Queue transaction candidates received from a specific peer,
                // whether inv-advertised ids or directly pushed transactions.
                // Per-peer accounting is enforced inside the downloader.
                Request::QueueFromPeer {
                    transactions,
                    source,
                } => {
                    trace!(
                        req_count = ?transactions.len(),
                        source = %queue_source_log_label(&source, self.expose_peer_addresses),
                        "got mempool QueueFromPeer request"
                    );

                    for gossiped_tx in transactions {
                        if storage.should_download_or_verify(gossiped_tx.id()).is_err() {
                            continue;
                        }
                        let _ = tx_downloads.download_if_needed_and_verify(
                            PendingTransaction::new(
                                gossiped_tx,
                                AdmissionOrigin::Peer(source.clone()),
                            ),
                            None,
                        );
                    }

                    self.update_metrics();

                    async move { Ok(Response::Queued(Vec::new())) }.boxed()
                }

                #[cfg(feature = "privacy-admission")]
                Request::QueuePrivate {
                    transaction,
                    context,
                } => {
                    let reservation = self.private_admission.reserve(transaction.clone(), context);
                    let response = match reservation {
                        Ok(PrivateReservationOutcome::Existing) => Ok(Response::PrivateQueued {
                            status: zakura_node_services::mempool::PrivateAdmissionStatus::Existing,
                            completion: None,
                        }),
                        Ok(PrivateReservationOutcome::Accepted(completion)) => {
                            match private_tx_downloads.download_if_needed_and_verify(
                                PendingTransaction::new(
                                    Gossip::Tx(transaction),
                                    AdmissionOrigin::PrivateLocal(context),
                                ),
                                None,
                            ) {
                                Ok(()) => Ok(Response::PrivateQueued {
                                    status: zakura_node_services::mempool::PrivateAdmissionStatus::Accepted,
                                    completion: Some(completion),
                                }),
                                Err(error) => {
                                    self.private_admission.cancel_reservation(context);
                                    Err(error.into())
                                }
                            }
                        }
                        Err(error) => Err(error.into()),
                    };
                    async move { response }.boxed()
                }

                #[cfg(feature = "privacy-admission")]
                Request::PrivatePoolDiagnostics => {
                    let diagnostics = self.private_admission.diagnostics();
                    async move { Ok(Response::PrivatePoolDiagnostics(diagnostics)) }.boxed()
                }

                #[cfg(feature = "privacy-admission")]
                Request::PromotePrivateDue => {
                    let current_tip = VerificationTip::new(
                        self.latest_chain_tip
                            .best_tip_height_and_hash()
                            .map(|(height, hash)| (hash, height)),
                    );
                    match self.private_admission.remove_expired(current_tip.height()) {
                        Ok(0) => {}
                        Ok(count) => {
                            self.update_metrics();
                            return async move {
                                Ok(Response::PrivatePromoted(
                                    zakura_node_services::mempool::PrivatePromotionOutcome::Terminal {
                                        count,
                                    },
                                ))
                            }
                            .boxed();
                        }
                        Err(error) => return async move { Err(error) }.boxed(),
                    }
                    let stale_records = self
                        .private_admission
                        .begin_revalidation(current_tip, false);
                    let excluded = stale_records
                        .iter()
                        .map(|record| record.admission_id())
                        .collect::<HashSet<_>>();
                    for record in stale_records {
                        let transaction_id = record.transaction_id();
                        let context = record.context();
                        let queue_result = private_tx_downloads.download_if_needed_and_verify(
                            PendingTransaction::new(
                                Gossip::Tx(record.transaction().transaction.clone()),
                                AdmissionOrigin::PrivateLocal(context),
                            ),
                            None,
                        );
                        if queue_result.is_err() {
                            self.private_admission
                                .revalidation_queue_failed(transaction_id, context);
                        }
                    }

                    let effects = self.private_admission.promote_due(storage, &excluded);
                    let outcome = effects.outcome;
                    if !effects.accepted.is_empty() {
                        pending_gossip_tx_ids.extend(effects.accepted.iter().copied());
                        if let Err(error) = self
                            .transaction_sender
                            .send(MempoolChange::added(effects.accepted))
                        {
                            return async move { Err(error.into()) }.boxed();
                        }
                    }
                    if !effects.evicted.is_empty() {
                        pending_gossip_tx_ids
                            .retain(|transaction_id| !effects.evicted.contains(transaction_id));
                        if let Err(error) = self
                            .transaction_sender
                            .send(MempoolChange::invalidated(effects.evicted))
                        {
                            return async move { Err(error.into()) }.boxed();
                        }
                    }
                    self.update_metrics();
                    async move { Ok(Response::PrivatePromoted(outcome)) }.boxed()
                }

                // Store successfully downloaded and verified transactions in the mempool
                Request::CheckForVerifiedTransactions => {
                    trace!(?req, "got mempool request");

                    // all the work for this request is done in poll_ready
                    async move { Ok(Response::CheckedForVerifiedTransactions) }.boxed()
                }

                // Summary statistics for the mempool: count, total size, and memory usage.
                //
                // Used by the `getmempoolinfo` RPC method
                Request::QueueStats => {
                    trace!(?req, "got mempool request");

                    let size = storage.transaction_count();

                    let bytes = storage.total_serialized_size();

                    let usage = bytes; // TODO: Placeholder, should be fixed later

                    // TODO: Set to Some(true) on regtest once network info is available.
                    let fully_notified = None;

                    trace!(size, bytes, usage, "answered mempool request");

                    async move {
                        Ok(Response::QueueStats {
                            size,
                            bytes,
                            usage,
                            fully_notified,
                        })
                    }
                    .boxed()
                }
                Request::UnspentOutput(outpoint) => {
                    trace!(?req, "got mempool request");

                    if storage.has_spent_outpoint(&outpoint) {
                        trace!(?req, "answered mempool request");

                        return async move {
                            Ok(Response::TransparentOutput(Some(CreatedOrSpent::Spent)))
                        }
                        .boxed();
                    }

                    if let Some((tx_version, output)) = storage
                        .transactions()
                        .get(&outpoint.hash)
                        .map(|tx| tx.transaction.transaction().clone())
                        .and_then(|tx| {
                            tx.outputs()
                                .get(outpoint.index as usize)
                                .map(|output| (tx.version(), output.clone()))
                        })
                    {
                        trace!(?req, "answered mempool request");

                        let last_seen_hash = *last_seen_tip_hash;
                        return async move {
                            Ok(Response::TransparentOutput(Some(CreatedOrSpent::Created {
                                output,
                                tx_version,
                                last_seen_hash,
                            })))
                        }
                        .boxed();
                    }

                    trace!(?req, "answered mempool request");

                    async move { Ok(Response::TransparentOutput(None)) }.boxed()
                }
            },
            ActiveState::Disabled => {
                // TODO: add the name of the request, but not the content,
                //       like the command() or Display impls of network requests
                trace!("got mempool request while mempool is disabled");

                // We can't return an error since that will cause a disconnection
                // by the peer connection handler. Therefore, return successful
                // empty responses.

                let resp = match req {
                    // Return empty responses for queries.
                    Request::TransactionIds | Request::TakePendingGossipTransactionIds { .. } => {
                        Response::TransactionIds(Default::default())
                    }

                    Request::TransactionsById(_) => Response::Transactions(Default::default()),
                    Request::TransactionsByMinedId(_) => Response::Transactions(Default::default()),
                    Request::TransactionWithDepsByMinedId(_)
                    | Request::AwaitOutput(_)
                    | Request::UnspentOutput(_) => {
                        return async move { Err(MempoolDisabledError.into()) }.boxed()
                    }

                    Request::FullTransactions => {
                        return async move { Err(MempoolDisabledError.into()) }.boxed()
                    }

                    Request::RejectedTransactionIds(_) => {
                        Response::RejectedTransactionIds(Default::default())
                    }

                    // Don't queue mempool candidates, because there is no queue.
                    Request::Queue(gossiped_txs) | Request::QueueFromCrawler(gossiped_txs) => {
                        Response::Queued(
                            // Special case; we can signal the error inside the response,
                            // because the inbound service ignores inner errors.
                            iter::repeat_n(MempoolError::Disabled, gossiped_txs.len())
                                .map(BoxError::from)
                                .map(Err)
                                .collect(),
                        )
                    }

                    // Drop peer-advertised txids when the mempool is disabled.
                    Request::QueueFromPeer { .. } => Response::Queued(Vec::new()),

                    #[cfg(feature = "privacy-admission")]
                    Request::QueuePrivate { .. } => {
                        return async move { Err(BoxError::from(MempoolError::Disabled)) }.boxed()
                    }

                    #[cfg(feature = "privacy-admission")]
                    Request::PrivatePoolDiagnostics => {
                        Response::PrivatePoolDiagnostics(self.private_admission.diagnostics())
                    }

                    #[cfg(feature = "privacy-admission")]
                    Request::PromotePrivateDue => Response::PrivatePromoted(
                        zakura_node_services::mempool::PrivatePromotionOutcome::Recoverable {
                            count: self.private_admission.diagnostics().transaction_count,
                        },
                    ),

                    // Check if the mempool should be enabled.
                    // This request makes sure mempools are debug-enabled in the acceptance tests.
                    Request::CheckForVerifiedTransactions => {
                        // all the work for this request is done in poll_ready
                        Response::CheckedForVerifiedTransactions
                    }

                    // Return empty mempool stats
                    Request::QueueStats => Response::QueueStats {
                        size: 0,
                        bytes: 0,
                        usage: 0,
                        fully_notified: None,
                    },
                };

                async move { Ok(resp) }.boxed()
            }
        }
    }
}

impl Drop for Mempool {
    fn drop(&mut self) {
        self.disable_metrics();
    }
}
