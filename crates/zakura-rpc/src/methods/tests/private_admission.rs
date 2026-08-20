use futures::FutureExt;
use tower::buffer::Buffer;

use zakura_chain::{chain_sync_status::MockSyncStatus, chain_tip::NoChainTip, parameters::Network};
use zakura_consensus::Request as ConsensusRequest;
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::{mempool, BoxError};
use zakura_state::{ReadRequest, ReadResponse, Request as StateRequest, Response as StateResponse};
use zakura_test::mock_service::{MockService, PanicAssertion};

use super::super::{RpcImpl, RpcServer};

#[cfg(feature = "privacy-admission")]
use super::super::PrivateRpcServer;
#[cfg(feature = "privacy-admission")]
use serde_json::json;
#[cfg(feature = "privacy-admission")]
use zakura_node_services::mempool::{PrivatePoolDiagnostics, PrivateWindowAggregate};

#[cfg(feature = "privacy-admission")]
mod enabled;

type Mempool = MockService<mempool::Request, mempool::Response, PanicAssertion, BoxError>;
type State = MockService<StateRequest, StateResponse, PanicAssertion, BoxError>;
type ReadState = MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>;
type Verifier = MockService<ConsensusRequest, zakura_chain::block::Hash, PanicAssertion, BoxError>;
type TestRpc = RpcImpl<
    Mempool,
    Buffer<State, StateRequest>,
    Buffer<ReadState, ReadRequest>,
    NoChainTip,
    MockAddressBookPeers,
    Verifier,
    MockSyncStatus,
>;

fn test_rpc() -> (Mempool, TestRpc, tokio::task::JoinHandle<()>) {
    let mempool = MockService::build().for_unit_tests();
    let state = MockService::build().for_unit_tests();
    let read_state = MockService::build().for_unit_tests();
    let verifier = MockService::build().for_unit_tests();
    let (_sender, receiver) = tokio::sync::watch::channel(None);
    let (rpc, queue_task) = RpcImpl::new(
        Network::Mainnet,
        Default::default(),
        false,
        "0.0.1",
        "RPC test",
        mempool.clone(),
        Buffer::new(state, 1),
        Buffer::new(read_state, 1),
        verifier,
        MockSyncStatus::default(),
        NoChainTip,
        MockAddressBookPeers::default(),
        receiver,
        None,
    );
    (mempool, rpc, queue_task)
}

#[tokio::test]
async fn private_rpc_registration_follows_feature_gate() {
    // Given: the RPC module generated for this build's feature set.
    let (_mempool, rpc, queue_task) = test_rpc();

    // When: its registered method names are inspected.
    let discovery = serde_json::to_string(
        &RpcServer::openrpc(&rpc).expect("OpenRPC discovery generation succeeds"),
    )
    .expect("OpenRPC discovery serializes");
    let methods = RpcServer::into_rpc(rpc.clone());
    #[cfg(feature = "privacy-admission")]
    let methods = {
        let mut methods = methods;
        methods
            .merge(PrivateRpcServer::into_rpc(rpc))
            .expect("private RPC method names are unique");
        methods
    };
    let names = methods.method_names().collect::<Vec<_>>();

    // Then: private methods exist exactly when privacy admission is enabled.
    assert_eq!(
        names.contains(&"sendprivatetransaction"),
        cfg!(feature = "privacy-admission")
    );
    assert_eq!(
        names.contains(&"getprivatepoolinfo"),
        cfg!(feature = "privacy-admission")
    );
    assert_eq!(
        discovery.contains("sendprivatetransaction"),
        cfg!(feature = "privacy-admission")
    );
    assert_eq!(
        discovery.contains("getprivatepoolinfo"),
        cfg!(feature = "privacy-admission")
    );
    assert!(names.contains(&"sendrawtransaction"));
    assert!(names.contains(&"getmempoolinfo"));
    assert!(queue_task.now_or_never().is_none());
}

#[cfg(feature = "privacy-admission")]
#[tokio::test]
async fn json_rpc_boundary_preserves_aggregates_with_completed_window() {
    // Given: a generated RPC module and one completed aggregate window.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let mut methods = RpcServer::into_rpc(rpc.clone());
    methods
        .merge(PrivateRpcServer::into_rpc(rpc))
        .expect("private RPC method names are unique");
    let request = json!({
        "jsonrpc": "2.0",
        "method": "getprivatepoolinfo",
        "params": [],
        "id": 8
    })
    .to_string();
    let diagnostics = PrivatePoolDiagnostics {
        transaction_count: 4,
        serialized_bytes: 5,
        max_transactions: 6,
        max_serialized_bytes: 7,
        embargoed_count: 8,
        eligible_count: 9,
        releasing_count: 10,
        scheduler_state: mempool::SchedulerState::Running,
        completed_window: Some(PrivateWindowAggregate {
            promoted: 3,
            recoverable: 2,
            terminal: 1,
        }),
    };

    // When: JSON-RPC dispatches the aggregate diagnostics request.
    let call = tokio::spawn(async move { methods.raw_json_request(&request, 1).await });
    mempool
        .expect_request(mempool::Request::PrivatePoolDiagnostics)
        .await
        .respond(mempool::Response::PrivatePoolDiagnostics(diagnostics));
    let (response, _subscriptions) = call
        .await
        .expect("the RPC task does not panic")
        .expect("the JSON-RPC request is valid");

    // Then: the wire response preserves aggregate diagnostics without identity or timing fields.
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response).expect("response is JSON"),
        json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction_count": 4,
                "serialized_bytes": 5,
                "max_transactions": 6,
                "max_serialized_bytes": 7,
                "embargoed_count": 8,
                "eligible_count": 9,
                "releasing_count": 10,
                "scheduler_state": "running",
                "completed_window": {
                    "promoted": 3,
                    "recoverable": 2,
                    "terminal": 1
                }
            },
            "id": 8
        })
    );
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}
