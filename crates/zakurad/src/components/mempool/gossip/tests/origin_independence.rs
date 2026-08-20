use super::*;

#[tokio::test]
async fn direct_peer_relay_and_private_promotion_share_an_origin_free_egress_request() {
    let _init_guard = zakura_test::init();

    // Given: each admission path has reduced its verified transaction to an Added event.
    let direct_tx_ids = test_tx_ids(1, 1);
    let peer_relay_tx_ids = test_tx_ids(1, 2);
    let private_promotion_tx_ids = test_tx_ids(1, 3);
    let pending_tx_ids: HashSet<UnminedTxId> = direct_tx_ids
        .iter()
        .chain(&peer_relay_tx_ids)
        .chain(&private_promotion_tx_ids)
        .copied()
        .collect();
    let (mempool, _limit_receiver) = mempool_service(vec![pending_tx_ids.clone()]);
    let (peer_set, mut advertised_receiver) = peer_set_service();
    let (sender, receiver) = broadcast::channel(MEMPOOL_CHANGE_CHANNEL_CAPACITY);
    for tx_ids in [direct_tx_ids, peer_relay_tx_ids, private_promotion_tx_ids] {
        sender
            .send(MempoolChange::added(tx_ids))
            .expect("receiver should be subscribed");
    }

    // When: the common gossip worker wakes for the accepted transactions.
    let gossip_task = tokio::spawn(run_mempool_transaction_id_gossip(
        receiver, peer_set, mempool,
    ));

    // Then: their pending IDs use the common request with no origin selector.
    assert_eq!(
        expect_advertised_transaction_ids(&mut advertised_receiver).await,
        pending_tx_ids
    );

    gossip_task.abort();
}
