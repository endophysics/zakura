use std::{collections::HashSet, time::Duration};

use proptest::prelude::*;
use tokio::time::timeout;
use tower::Service;

use zakura_chain::{parameters::Network, transaction};

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION,
    peer::{MinimumPeerVersion, ReceiveRequestAttempt},
    Request, Response,
};

use super::{PeerSetBuilder, PeerVersions};

#[test]
fn transaction_advertisement_broadcasts_to_peers() -> Result<(), TestCaseError> {
    let request = Request::AdvertiseTransactionIds(
        HashSet::from([transaction::UnminedTxId::Legacy(transaction::Hash([0; 32]))]),
        None,
    );
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();
    let peer_versions = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION; 3],
    };

    runtime.block_on(async move {
        let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
        let (minimum_peer_version, _best_tip_height) =
            MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(usize::MAX)
            .build();
        let active_peers = super::prop::check_if_only_up_to_date_peers_are_live(
            &mut peer_set,
            &mut handles,
            CURRENT_NETWORK_PROTOCOL_VERSION,
        )?;
        let expected_broadcast_peers = peer_set.number_of_peers_to_broadcast();

        let broadcast_handle = tokio::spawn(peer_set.call(request.clone()));
        let received = timeout(Duration::from_secs(1), async {
            let mut received = 0;
            while received < expected_broadcast_peers {
                for handle in &mut handles {
                    if let ReceiveRequestAttempt::Request(client_request) =
                        handle.try_to_receive_outbound_client_request()
                    {
                        prop_assert_eq!(client_request.request, request.clone());
                        client_request
                            .tx
                            .send(Ok(Response::Nil))
                            .expect("mock peer response receiver remains active");
                        received += 1;
                    }
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, TestCaseError>(received)
        })
        .await
        .expect("broadcast requests should reach all selected peers")?;
        broadcast_handle
            .await
            .expect("broadcast task should not panic")
            .expect("broadcast should succeed");

        prop_assert_eq!(active_peers, 3);
        prop_assert_eq!(received, expected_broadcast_peers);
        Ok(())
    })
}
