use super::*;
use std::collections::HashSet;

struct LifecycleHarness {
    mempool: Mempool,
    state_service: StateService,
    chain_tip_change: ChainTipChange,
    verifier: MockTxVerifier,
    events: tokio::sync::broadcast::Receiver<MempoolChange>,
}

impl LifecycleHarness {
    async fn at_tip(network: &Network, blocks: &[Arc<Block>]) -> Self {
        Self::at_tip_with_config(network, blocks, mempool::Config::default()).await
    }

    async fn at_tip_with_config(
        network: &Network,
        blocks: &[Arc<Block>],
        config: mempool::Config,
    ) -> Self {
        let (
            mut mempool,
            _,
            mut state_service,
            mut chain_tip_change,
            verifier,
            mut recent_syncs,
            events,
        ) = setup_with_mempool_config(network, config, false).await;
        for block in blocks {
            commit_block_and_wait_for_tip_change(
                &mut state_service,
                &mut chain_tip_change,
                block.clone(),
            )
            .await;
        }
        mempool.enable(&mut recent_syncs).await;
        Self {
            mempool,
            state_service,
            chain_tip_change,
            verifier,
            events,
        }
    }

    async fn retain(&mut self, verified: VerifiedUnminedTx, context: AdmissionContext) {
        let transaction = verified.transaction.clone();
        let queue =
            self.mempool
                .ready()
                .await
                .expect("mempool is ready")
                .call(Request::QueuePrivate {
                    transaction,
                    context,
                });
        let verify = self
            .verifier
            .expect_request_that(|_| true)
            .map(|responder| {
                responder.respond(transaction::Response::Mempool {
                    transaction: verified,
                    spent_mempool_outpoints: Vec::new(),
                });
            });
        let (response, _) = futures::join!(queue, verify);
        let Response::PrivateQueued {
            completion: Some(mut completion),
            ..
        } = response.expect("private queue succeeds")
        else {
            panic!("new private queue returns completion");
        };
        timeout(Duration::from_secs(3), async {
            loop {
                self.mempool.dummy_call().await;
                match completion.try_recv() {
                    Ok(result) => break result.expect("private retention succeeds"),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        tokio::task::yield_now().await;
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        panic!("private completion remains open")
                    }
                }
            }
        })
        .await
        .expect("private retention is timely");
    }

    async fn commit(&mut self, block: Arc<Block>) {
        commit_block_and_wait_for_tip_change(
            &mut self.state_service,
            &mut self.chain_tip_change,
            block,
        )
        .await;
        self.mempool.dummy_call().await;
    }

    async fn finish_revalidation(&mut self) {
        let responder = timeout(
            Duration::from_secs(3),
            self.verifier.expect_request_that(|_| true),
        )
        .await
        .expect("retained revalidation reaches verifier");
        let transaction = responder
            .request()
            .clone()
            .mempool_transaction()
            .expect("revalidation is a mempool request");
        responder.respond(transaction::Response::Mempool {
            transaction: VerifiedUnminedTx::new(
                transaction,
                Amount::try_from(1_000_000).expect("valid test fee"),
                0,
                0,
                Arc::new(Vec::new()),
            )
            .expect("mock revalidation succeeds"),
            spent_mempool_outpoints: Vec::new(),
        });
        self.drain_revalidation().await;
    }

    async fn drain_revalidation(&mut self) {
        timeout(Duration::from_secs(3), async {
            loop {
                self.mempool.dummy_call().await;
                if self
                    .mempool
                    .private_tx_downloads()
                    .transaction_requests()
                    .count()
                    == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained revalidation completion is timely");
    }
}

fn immediate_release_config() -> mempool::Config {
    let release = private_pool::PrivateReleaseConfig::new(
        Duration::from_nanos(1),
        Duration::from_nanos(1),
        Duration::from_nanos(1),
    )
    .expect("test release policy is valid");
    mempool::Config {
        private_pool: private_pool::PrivatePoolConfig::new(10, usize::MAX, release)
            .expect("test private capacity is valid"),
        tx_cost_limit: u64::MAX,
        ..Default::default()
    }
}

fn scheduled_release_config() -> mempool::Config {
    let release = private_pool::PrivateReleaseConfig::new(
        Duration::from_millis(10),
        Duration::from_nanos(1),
        Duration::from_secs(1),
    )
    .expect("test release policy is valid");
    mempool::Config {
        private_pool: private_pool::PrivatePoolConfig::new(10, usize::MAX, release)
            .expect("test private capacity is valid"),
        tx_cost_limit: u64::MAX,
        ..Default::default()
    }
}

fn private_capacity_config(max_transactions: usize) -> mempool::Config {
    mempool::Config {
        private_pool: private_pool::PrivatePoolConfig::new(
            max_transactions,
            usize::MAX,
            private_pool::PrivateReleaseConfig::default(),
        )
        .expect("test private capacity is valid"),
        tx_cost_limit: u64::MAX,
        ..Default::default()
    }
}

fn empty_v5_transaction(expiry_height: u32) -> zakura_chain::transaction::UnminedTx {
    Transaction::V5 {
        network_upgrade: zakura_chain::parameters::NetworkUpgrade::Nu5,
        lock_time: zakura_chain::transaction::LockTime::min_lock_time_timestamp(),
        expiry_height: Height(expiry_height),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
    .into()
}

fn public_transaction_id(index: u64) -> zakura_chain::transaction::UnminedTxId {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    zakura_chain::transaction::UnminedTxId::from_legacy_id(zakura_chain::transaction::Hash(bytes))
}

fn standard_transactions(network: &Network, count: usize) -> Vec<VerifiedUnminedTx> {
    let transactions = network
        .unmined_transactions_in_blocks(..)
        .filter(|transaction| {
            transaction
                .transaction
                .transaction()
                .outputs()
                .iter()
                .all(|output| !output.is_dust())
        })
        .take(count)
        .collect::<Vec<_>>();
    assert_eq!(transactions.len(), count, "test vectors cover the batch");
    transactions
}

fn verified_from(block: &Block) -> VerifiedUnminedTx {
    let transaction = block.transactions[0].clone().into();
    VerifiedUnminedTx::new(
        transaction,
        Amount::try_from(1_000_000).expect("valid test fee"),
        0,
        0,
        Arc::new(Vec::new()),
    )
    .expect("generated transaction passes mock policy")
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_public_verification_keeps_private_admission_operational() -> Result<(), Report> {
    // Given: every public downloader slot is occupied by a distinct transaction.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness =
        LifecycleHarness::at_tip_with_config(&network, &blocks, private_capacity_config(1)).await;
    let public_transactions = (0..mempool::downloads::MAX_INBOUND_CONCURRENCY)
        .map(|index| {
            Gossip::Id(public_transaction_id(
                u64::try_from(index).expect("test index fits in u64"),
            ))
        })
        .collect();
    let Response::Queued(public_results) = harness
        .mempool
        .ready()
        .await
        .expect("mempool is ready")
        .call(Request::Queue(public_transactions))
        .await
        .expect("public queue request succeeds")
    else {
        panic!("public queue returns per-transaction results");
    };
    assert!(public_results.iter().all(Result::is_ok));
    assert_eq!(
        harness.mempool.tx_downloads().in_flight(),
        mempool::downloads::MAX_INBOUND_CONCURRENCY
    );
    assert_eq!(harness.mempool.private_tx_downloads().in_flight(), 0);

    // When: one private transaction is submitted while public verification is saturated.
    let private_response = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::QueuePrivate {
            transaction: empty_v5_transaction(1),
            context: admission_context(1_000),
        })
        .await;

    // Then: independent private verification capacity accepts the transaction.
    assert!(matches!(
        private_response,
        Ok(Response::PrivateQueued {
            status: zakura_node_services::mempool::PrivateAdmissionStatus::Accepted,
            completion: Some(_),
        })
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_private_verification_keeps_public_admission_operational() -> Result<(), Report> {
    // Given: private verification occupies every private verifier slot.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let private_capacity = VERIFIER_BUFFER_BOUND - 1;
    let mut harness = LifecycleHarness::at_tip_with_config(
        &network,
        &blocks,
        private_capacity_config(mempool::downloads::MAX_INBOUND_CONCURRENCY),
    )
    .await;
    for index in 0..private_capacity {
        let response = harness
            .mempool
            .ready()
            .await
            .expect("mempool is ready")
            .call(Request::QueuePrivate {
                transaction: empty_v5_transaction(
                    u32::try_from(index + 1).expect("test index fits in u32"),
                ),
                context: admission_context(
                    u64::try_from(index + 1).expect("test index fits in u64"),
                ),
            })
            .await
            .expect("private queue request succeeds");
        assert!(matches!(
            response,
            Response::PrivateQueued {
                status: zakura_node_services::mempool::PrivateAdmissionStatus::Accepted,
                completion: Some(_),
            }
        ));
    }
    assert_eq!(harness.mempool.tx_downloads().in_flight(), 0);
    assert_eq!(
        harness.mempool.private_tx_downloads().in_flight(),
        private_capacity
    );

    // When: one public transaction is submitted while private verification is saturated.
    let Response::Queued(mut public_results) = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::Queue(vec![Gossip::Id(public_transaction_id(
            u64::MAX,
        ))]))
        .await
        .expect("public queue request succeeds")
    else {
        panic!("public queue returns per-transaction results");
    };

    // Then: independent public verification capacity accepts the transaction.
    assert_eq!(public_results.len(), 1);
    assert!(public_results.remove(0).is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_private_verification_preserves_public_completion() -> Result<(), Report> {
    // Given: private admissions fill every verifier slot the private stream allows.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness = LifecycleHarness::at_tip_with_config(
        &network,
        &blocks,
        private_capacity_config(VERIFIER_BUFFER_BOUND),
    )
    .await;
    let mut held_private_responders = Vec::new();
    for index in 0..VERIFIER_BUFFER_BOUND {
        let response = harness
            .mempool
            .ready()
            .await
            .expect("mempool is ready")
            .call(Request::QueuePrivate {
                transaction: empty_v5_transaction(
                    u32::try_from(index + 1).expect("test index fits in u32"),
                ),
                context: admission_context(
                    u64::try_from(index + 1).expect("test index fits in u64"),
                ),
            })
            .await;

        match response {
            Ok(Response::PrivateQueued {
                status: zakura_node_services::mempool::PrivateAdmissionStatus::Accepted,
                completion: Some(_),
            }) => {
                held_private_responders.push(
                    timeout(
                        Duration::from_secs(3),
                        harness.verifier.expect_request_that(|_| true),
                    )
                    .await
                    .expect("accepted private verification reaches the verifier"),
                );
            }
            other => assert!(matches!(
                other.unbox_mempool_error(),
                MempoolError::FullQueue
            )),
        }
    }

    let public = standard_transactions(&network, 1)
        .pop()
        .expect("one standard public transaction");
    let public_id = public.transaction.id();

    // When: a public transaction is submitted while every allowed private verifier is unresolved.
    let Response::Queued(mut queued) = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::Queue(vec![Gossip::Tx(public.transaction.clone())]))
        .await
        .expect("public queue succeeds")
    else {
        panic!("public queue returns its ordinary response");
    };
    let mut completion = queued.remove(0).expect("public transaction is queued");
    let public_responder = timeout(
        Duration::from_secs(3),
        harness.verifier.expect_request_that(|_| true),
    )
    .await
    .expect("public verification reaches the verifier");
    public_responder.respond(transaction::Response::Mempool {
        transaction: public,
        spent_mempool_outpoints: Vec::new(),
    });

    timeout(Duration::from_secs(3), async {
        loop {
            harness.mempool.dummy_call().await;
            match completion.try_recv() {
                Ok(result) => break result,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    panic!("public completion remains open")
                }
            }
        }
    })
    .await
    .expect("public insertion is timely")
    .expect("public insertion succeeds");

    // Then: one shared verifier slot remained public, and insertion completed normally.
    assert_eq!(held_private_responders.len(), VERIFIER_BUFFER_BOUND - 1);
    assert!(harness.mempool.storage().tx_ids().any(|id| id == public_id));
    assert_eq!(
        harness.events.try_recv(),
        Ok(MempoolChange::added(HashSet::from([public_id])))
    );
    for responder in held_private_responders {
        responder.respond(Err(TransactionError::BadBalance));
    }
    timeout(Duration::from_secs(3), async {
        loop {
            harness.mempool.dummy_call().await;
            if harness.mempool.private_tx_downloads().in_flight() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("private verifier teardown is timely");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_private_requests_are_rejected_after_service_close() -> Result<(), Report> {
    // Given: a retained due transaction and admission and promotion requests buffered before use.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness =
        LifecycleHarness::at_tip_with_config(&network, &blocks, immediate_release_config()).await;
    let mut transactions = standard_transactions(&network, 2).into_iter();
    let retained = transactions.next().expect("retained transaction");
    let queued = transactions.next().expect("queued transaction").transaction;
    harness.retain(retained, admission_context(900)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let diagnostics_before = harness.mempool.private_diagnostics();
    let LifecycleHarness {
        mempool,
        mut verifier,
        mut events,
        ..
    } = harness;
    let control = mempool.private_lifecycle_control();
    let (mut buffered, worker) = tower::buffer::Buffer::pair(
        tower::util::BoxService::new(mempool),
        mempool::downloads::MAX_INBOUND_CONCURRENCY,
    );
    let admission = buffered
        .ready()
        .await
        .expect("buffer accepts private admission")
        .call(Request::QueuePrivate {
            transaction: queued,
            context: admission_context(901),
        });
    let promotion = buffered
        .ready()
        .await
        .expect("buffer accepts private promotion")
        .call(Request::PromotePrivateDue);

    // When: shutdown closes private operations before the buffer worker handles either request.
    control.close();
    let worker = tokio::spawn(worker);
    let (admission, promotion) = futures::join!(admission, promotion);

    // Then: both fail closed without private, public, verification, event, gossip, or counter changes.
    assert!(matches!(
        admission.unbox_mempool_error(),
        MempoolError::PrivateOperationsClosed
    ));
    assert!(matches!(
        promotion.unbox_mempool_error(),
        MempoolError::PrivateOperationsClosed
    ));
    verifier.expect_no_requests().await;
    let diagnostics = buffered
        .ready()
        .await
        .expect("buffer remains ready for diagnostics")
        .call(Request::PrivatePoolDiagnostics)
        .await
        .expect("aggregate diagnostics remain available");
    assert!(matches!(
        diagnostics,
        Response::PrivatePoolDiagnostics(diagnostics) if diagnostics == diagnostics_before
    ));
    let public_ids = buffered
        .ready()
        .await
        .expect("buffer remains ready for public queries")
        .call(Request::TransactionIds)
        .await
        .expect("public query succeeds");
    assert!(matches!(public_ids, Response::TransactionIds(ids) if ids.is_empty()));
    let pending_gossip = buffered
        .ready()
        .await
        .expect("buffer remains ready for gossip queries")
        .call(Request::TakePendingGossipTransactionIds { limit: usize::MAX })
        .await
        .expect("gossip query succeeds");
    assert!(matches!(pending_gossip, Response::TransactionIds(ids) if ids.is_empty()));
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    worker.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_promotes_through_the_buffered_mempool_request_seam() -> Result<(), Report> {
    // Given: one retained transaction and the same buffered mempool shape used by startup.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let config = scheduled_release_config();
    let mut harness = LifecycleHarness::at_tip_with_config(&network, &blocks, config).await;
    let transaction = standard_transactions(&network, 1)
        .pop()
        .expect("one standard transaction");
    let transaction_id = transaction.transaction.id();
    harness.retain(transaction, admission_context(1)).await;
    let LifecycleHarness {
        mut mempool,
        mut events,
        ..
    } = harness;
    let (state_sender, state_receiver) =
        tokio::sync::watch::channel(zakura_node_services::mempool::SchedulerState::Idle);
    mempool.set_private_scheduler_state(state_receiver);
    let release_timing = mempool.private_release_timing();
    let mempool = tower::util::BoxService::new(mempool);
    let mempool = ServiceBuilder::new()
        .buffer(mempool::downloads::MAX_INBOUND_CONCURRENCY)
        .service(mempool);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let scheduler = PrivateReleaseScheduler::spawn(
        mempool.clone(),
        release_timing,
        state_sender,
        shutdown.clone(),
    );

    // When: the scheduler's first release tick reaches the real service.
    let event = timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("scheduled promotion is timely")
        .expect("mempool event channel remains open");

    // Then: the existing promotion path commits publicly and reports aggregate lifecycle state.
    assert_eq!(event, MempoolChange::added(HashSet::from([transaction_id])));
    let running = mempool
        .clone()
        .oneshot(Request::PrivatePoolDiagnostics)
        .await
        .expect("diagnostics request succeeds");
    assert!(matches!(
        running,
        Response::PrivatePoolDiagnostics(diagnostics)
            if diagnostics.scheduler_state
                == zakura_node_services::mempool::SchedulerState::Running
                && diagnostics.transaction_count == 0
    ));
    shutdown.cancel();
    assert!(scheduler
        .await
        .expect("scheduler task does not panic")
        .is_ok());
    let stopping = mempool
        .oneshot(Request::PrivatePoolDiagnostics)
        .await
        .expect("diagnostics request succeeds after cancellation");
    assert!(matches!(
        stopping,
        Response::PrivatePoolDiagnostics(diagnostics)
            if diagnostics.scheduler_state
                == zakura_node_services::mempool::SchedulerState::Stopping
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn promotion_commits_one_complete_public_event_and_pending_gossip_set() -> Result<(), Report>
{
    // Given: two due private records verified at the exact latest tip.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness =
        LifecycleHarness::at_tip_with_config(&network, &blocks, immediate_release_config()).await;
    let mut transactions = standard_transactions(&network, 3).into_iter();
    let public = transactions.next().expect("public transaction");
    let private = transactions.collect::<Vec<_>>();
    let public_id = public.transaction.id();
    let private_cost = private.iter().map(VerifiedUnminedTx::cost).sum();
    let expected_ids = private
        .iter()
        .map(|transaction| transaction.transaction.id())
        .collect::<HashSet<_>>();
    harness
        .mempool
        .storage()
        .insert(public, Vec::new(), None)
        .expect("public fixture insertion succeeds");
    harness
        .mempool
        .storage()
        .configure_private_promotion_eviction(private_cost, [public_id.mined_id()]);
    for (index, transaction) in private.into_iter().enumerate() {
        harness
            .retain(
                transaction,
                admission_context(u64::try_from(index).expect("test index fits")),
            )
            .await;
    }
    tokio::time::sleep(Duration::from_millis(1)).await;

    // When: the internal service promotes the complete due set.
    let response = harness
        .mempool
        .ready()
        .await
        .expect("mempool is ready")
        .call(Request::PromotePrivateDue)
        .await
        .expect("promotion request succeeds");

    // Then: one aggregate response, one Added event, and ordinary victim invalidation commit.
    assert!(matches!(
        response,
        Response::PrivatePromoted(
            zakura_node_services::mempool::PrivatePromotionOutcome::Promoted { count: 2 }
        )
    ));
    assert_eq!(
        harness.mempool.storage().tx_ids().collect::<HashSet<_>>(),
        expected_ids
    );
    assert_eq!(harness.mempool.private_diagnostics().transaction_count, 0);
    assert_eq!(
        harness.events.try_recv(),
        Ok(MempoolChange::added(expected_ids.clone()))
    );
    assert_eq!(
        harness.events.try_recv(),
        Ok(MempoolChange::invalidated(HashSet::from([public_id])))
    );
    assert!(matches!(
        harness.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let pending = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::TakePendingGossipTransactionIds { limit: usize::MAX })
        .await
        .expect("pending gossip query succeeds");
    assert!(matches!(pending, Response::TransactionIds(ids) if ids == expected_ids));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_conflict_has_no_public_effect_and_remaining_candidate_promotes(
) -> Result<(), Report> {
    // Given: a two-record due batch whose first transaction already exists publicly.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness =
        LifecycleHarness::at_tip_with_config(&network, &blocks, immediate_release_config()).await;
    let transactions = standard_transactions(&network, 2);
    let remaining_id = transactions[1].transaction.id();
    for (index, transaction) in transactions.iter().cloned().enumerate() {
        harness
            .retain(
                transaction,
                admission_context(u64::try_from(index).expect("test index fits")),
            )
            .await;
    }
    harness
        .mempool
        .storage()
        .insert(transactions[0].clone(), Vec::new(), None)
        .expect("public fixture insertion succeeds");
    let public_before = harness.mempool.storage().tx_ids().collect::<HashSet<_>>();
    tokio::time::sleep(Duration::from_millis(1)).await;

    // When: complete-batch public preflight rejects promotion.
    let response = harness
        .mempool
        .ready()
        .await
        .expect("mempool is ready")
        .call(Request::PromotePrivateDue)
        .await
        .expect("recoverable promotion returns a response");

    // Then: only the conflicting private record terminates without a public side effect.
    assert!(matches!(
        response,
        Response::PrivatePromoted(
            zakura_node_services::mempool::PrivatePromotionOutcome::Terminal { count: 1 }
        )
    ));
    assert_eq!(
        harness.mempool.storage().tx_ids().collect::<HashSet<_>>(),
        public_before
    );
    assert_eq!(harness.mempool.private_diagnostics().transaction_count, 1);
    assert!(matches!(
        harness.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let pending = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::TakePendingGossipTransactionIds { limit: usize::MAX })
        .await
        .expect("pending gossip query succeeds");
    assert!(matches!(pending, Response::TransactionIds(ids) if ids.is_empty()));

    // When: the scheduler seam creates a fresh complete preparation.
    let retry = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::PromotePrivateDue)
        .await
        .expect("remaining promotion succeeds");

    // Then: the unaffected candidate promotes through the ordinary event and gossip path.
    assert!(matches!(
        retry,
        Response::PrivatePromoted(
            zakura_node_services::mempool::PrivatePromotionOutcome::Promoted { count: 1 }
        )
    ));
    assert_eq!(
        harness.events.try_recv(),
        Ok(MempoolChange::added(HashSet::from([remaining_id])))
    );
    let pending = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::TakePendingGossipTransactionIds { limit: usize::MAX })
        .await
        .expect("pending gossip query succeeds");
    assert!(
        matches!(pending, Response::TransactionIds(ids) if ids == HashSet::from([remaining_id]))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn promotion_terminally_expires_at_the_public_tip_boundary() -> Result<(), Report> {
    // Given: one due private transaction expiring exactly at the current public tip.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 2);
    let mut harness =
        LifecycleHarness::at_tip_with_config(&network, &blocks, immediate_release_config()).await;
    let mut transaction = network
        .unmined_transactions_in_blocks(..)
        .find(|transaction| {
            transaction
                .transaction
                .transaction()
                .expiry_height()
                .is_some()
                && transaction
                    .transaction
                    .transaction()
                    .outputs()
                    .iter()
                    .all(|output| !output.is_dust())
        })
        .expect("test vectors contain an expiring standard transaction");
    let mut inner = transaction.transaction.transaction().as_ref().clone();
    *inner.expiry_height_mut() = Height(1);
    transaction.transaction = inner.into();
    harness.retain(transaction, admission_context(800)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;

    // When: promotion checks expiry before stale revalidation or public preflight.
    let response = harness
        .mempool
        .ready()
        .await
        .expect("mempool is ready")
        .call(Request::PromotePrivateDue)
        .await
        .expect("terminal expiry returns a response");

    // Then: private/core state terminates without verifier, public, event, or gossip effects.
    assert!(matches!(
        response,
        Response::PrivatePromoted(
            zakura_node_services::mempool::PrivatePromotionOutcome::Terminal { count: 1 }
        )
    ));
    let diagnostics = harness.mempool.private_diagnostics();
    assert_eq!(diagnostics.transaction_count, 0);
    assert_eq!(diagnostics.terminal_count, 1);
    assert_eq!(harness.mempool.storage().transaction_count(), 0);
    assert_eq!(
        harness
            .mempool
            .private_tx_downloads()
            .transaction_requests()
            .count(),
        0
    );
    assert!(matches!(
        harness.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let pending = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::TakePendingGossipTransactionIds { limit: usize::MAX })
        .await
        .expect("pending gossip query succeeds");
    assert!(matches!(pending, Response::TransactionIds(ids) if ids.is_empty()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn promotion_request_does_not_change_public_queue_behavior() -> Result<(), Report> {
    // Given: an enabled mempool after a NoDue promotion request.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 1);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks).await;
    assert!(matches!(
        harness
            .mempool
            .ready()
            .await
            .expect("mempool is ready")
            .call(Request::PromotePrivateDue)
            .await
            .expect("NoDue promotion succeeds"),
        Response::PrivatePromoted(zakura_node_services::mempool::PrivatePromotionOutcome::NoDue)
    ));
    let verified = standard_transactions(&network, 1)
        .pop()
        .expect("one standard transaction");
    let transaction_id = verified.transaction.id();

    // When: the ordinary public Queue path verifies and inserts that transaction.
    let queue = harness
        .mempool
        .ready()
        .await
        .expect("mempool remains ready")
        .call(Request::Queue(vec![Gossip::Tx(
            verified.transaction.clone(),
        )]));
    let verify = harness
        .verifier
        .expect_request_that(|_| true)
        .map(|responder| {
            responder.respond(transaction::Response::Mempool {
                transaction: verified,
                spent_mempool_outpoints: Vec::new(),
            });
        });
    let (response, _) = futures::join!(queue, verify);
    let Response::Queued(mut queued) = response.expect("public queue succeeds") else {
        panic!("public queue returns its ordinary response");
    };
    let mut completion = queued.remove(0).expect("public transaction is queued");
    timeout(Duration::from_secs(3), async {
        loop {
            harness.mempool.dummy_call().await;
            match completion.try_recv() {
                Ok(result) => break result,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    panic!("public completion remains open")
                }
            }
        }
    })
    .await
    .expect("public insertion is timely")
    .expect("public insertion succeeds");

    // Then: the normal public insertion and Added event remain unchanged.
    assert!(harness
        .mempool
        .storage()
        .tx_ids()
        .any(|id| id == transaction_id));
    assert_eq!(
        harness.events.try_recv(),
        Ok(MempoolChange::added(HashSet::from([transaction_id])))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_grow_revalidates_and_preserves_original_schedule() -> Result<(), Report> {
    // Given: a private record retained at genesis with an immutable core schedule.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 3);
    let context = admission_context(701);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks[..1]).await;
    harness.retain(verified_from(&blocks[2]), context).await;
    let schedule = harness.mempool.private_schedule(context.admission_id);

    // When: the canonical chain grows and retained verification succeeds at the new tip.
    harness.commit(blocks[1].clone()).await;
    harness.finish_revalidation().await;

    // Then: the retained record is replaced at the current tip without core re-admission.
    assert_eq!(
        harness
            .mempool
            .private_record(context.admission_id)
            .expect("record remains retained")
            .verification_tip()
            .hash_and_height(),
        Some((blocks[1].hash(), Height(1)))
    );
    assert_eq!(
        harness.mempool.private_schedule(context.admission_id),
        schedule
    );
    assert_eq!(harness.mempool.storage().transaction_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_reset_revalidates_even_when_pool_survives_active_state() -> Result<(), Report> {
    // Given: a retained private record immediately before a network-upgrade Reset.
    let network = nu_activation_test_network();
    let blocks = generate_test_chain(&network, 6);
    let context = admission_context(702);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks[..3]).await;
    harness.retain(verified_from(&blocks[5]), context).await;

    // When: Reset rebuilds ActiveState and retained revalidation succeeds.
    harness.commit(blocks[3].clone()).await;
    harness.finish_revalidation().await;

    // Then: the retained record has the Reset tip and remains privately capacity-owned.
    assert_eq!(
        harness
            .mempool
            .private_record(context.admission_id)
            .expect("record remains retained")
            .verification_tip()
            .hash_and_height(),
        Some((blocks[3].hash(), Height(3)))
    );
    assert_eq!(harness.mempool.private_diagnostics().transaction_count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_grow_invalid_is_terminally_rejected() -> Result<(), Report> {
    // Given: one retained private record at genesis.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 3);
    let context = admission_context(703);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks[..1]).await;
    harness.retain(verified_from(&blocks[2]), context).await;
    // When: Grow revalidation returns a deterministic consensus failure.
    harness.commit(blocks[1].clone()).await;
    let responder = harness.verifier.expect_request_that(|_| true).await;
    responder.respond(Err(TransactionError::BadBalance));
    harness.drain_revalidation().await;

    // Then: core rejection removes the private record without any public effect.
    assert!(harness
        .mempool
        .private_record(context.admission_id)
        .is_none());
    let diagnostics = harness.mempool.private_diagnostics();
    assert_eq!(diagnostics.terminal_count, 1);
    assert_eq!(diagnostics.recoverable_count, 0);
    assert_eq!(harness.mempool.storage().transaction_count(), 0);
    assert!(matches!(
        harness.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_grow_revalidation_is_independent_of_public_duplicate() -> Result<(), Report> {
    // Given: a retained transaction already occupying the public downloader.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 3);
    let context = admission_context(704);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks[..1]).await;
    let verified = verified_from(&blocks[2]);
    harness.retain(verified.clone(), context).await;
    harness
        .mempool
        .ready()
        .await
        .expect("mempool is ready")
        .call(Request::Queue(vec![Gossip::Tx(
            verified.transaction.clone(),
        )]))
        .await
        .expect("public duplicate occupies downloader");

    // When: Grow queues retained revalidation for the same transaction.
    harness.commit(blocks[1].clone()).await;

    // Then: both streams own one task and private revalidation remains in progress.
    assert!(harness
        .mempool
        .private_record(context.admission_id)
        .is_some());
    assert_eq!(harness.mempool.tx_downloads().in_flight(), 1);
    assert_eq!(harness.mempool.private_tx_downloads().in_flight(), 1);
    assert_eq!(harness.mempool.private_diagnostics().recoverable_count, 0);
    assert!(!harness
        .mempool
        .private_batch_available(context.admission_id));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_mined_on_grow_is_removed_without_public_effects() -> Result<(), Report> {
    // Given: a retained private transaction that appears in the next canonical block.
    let network = Network::Mainnet;
    let blocks = generate_test_chain(&network, 2);
    let context = admission_context(705);
    let mut harness = LifecycleHarness::at_tip(&network, &blocks[..1]).await;
    harness.retain(verified_from(&blocks[1]), context).await;

    // When: Grow mines the retained transaction.
    harness.commit(blocks[1].clone()).await;

    // Then: private ownership terminates without public storage or event effects.
    assert!(harness
        .mempool
        .private_record(context.admission_id)
        .is_none());
    let diagnostics = harness.mempool.private_diagnostics();
    assert_eq!(diagnostics.transaction_count, 0);
    assert_eq!(diagnostics.terminal_count, 1);
    assert_eq!(harness.mempool.storage().transaction_count(), 0);
    assert!(matches!(
        harness.events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    Ok(())
}
