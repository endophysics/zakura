use std::sync::Arc;

use futures::FutureExt;
use hex::ToHex;
use jsonrpsee_types::ErrorCode;
use serde_json::json;

use zakura_chain::{
    serialization::ZcashDeserialize,
    transaction::{Transaction, UnminedTx},
};
use zakura_node_services::mempool::{
    self, AdmissionContext, AdmissionId, AdmissionPolicy, PrivateAdmissionStatus,
    PrivatePoolDiagnostics, SchedulerState,
};

use super::{test_rpc, PrivateRpcServer, RpcServer};

const ADMISSION_ID: AdmissionId = AdmissionId(9_223_372_036_854_775_001);

fn transaction_fixture() -> (String, UnminedTx) {
    let transaction = Arc::<Transaction>::zcash_deserialize(&**zakura_test::vectors::DUMMY_TX1)
        .expect("the fixed transaction vector is valid");
    (
        zakura_test::vectors::DUMMY_TX1.encode_hex::<String>(),
        transaction.into(),
    )
}

fn context() -> AdmissionContext {
    AdmissionContext {
        admission_id: ADMISSION_ID,
        policy: AdmissionPolicy::FixedEpoch,
    }
}

#[tokio::test]
async fn json_rpc_boundary_deserializes_typed_admission_id() {
    // Given: a generated RPC module and a numeric caller-supplied admission ID.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let mut methods = RpcServer::into_rpc(rpc.clone());
    methods
        .merge(PrivateRpcServer::into_rpc(rpc))
        .expect("private RPC method names are unique");
    let request = json!({
        "jsonrpc": "2.0",
        "method": "sendprivatetransaction",
        "params": [raw, ADMISSION_ID.0],
        "id": 7
    })
    .to_string();

    // When: JSON-RPC deserializes and dispatches the request.
    let call = tokio::spawn(async move { methods.raw_json_request(&request, 1).await });
    mempool
        .expect_request(mempool::Request::QueuePrivate {
            transaction,
            context: context(),
        })
        .await
        .respond(mempool::Response::PrivateQueued {
            status: PrivateAdmissionStatus::Existing,
            completion: None,
        });
    let (response, _subscriptions) = call
        .await
        .expect("the RPC task does not panic")
        .expect("the JSON-RPC request is valid");

    // Then: the wire response contains only the status and request correlation ID.
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response).expect("response is JSON"),
        json!({"jsonrpc": "2.0", "result": "existing", "id": 7})
    );
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn accepted_private_submission_waits_for_completion() {
    // Given: a valid raw transaction and a pending verification completion.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let (completion_sender, completion) = tokio::sync::oneshot::channel();

    // When: the authenticated method submits the transaction.
    let call = tokio::spawn(async move { rpc.send_private_transaction(raw, ADMISSION_ID).await });
    mempool
        .expect_request(mempool::Request::QueuePrivate {
            transaction,
            context: context(),
        })
        .await
        .respond(mempool::Response::PrivateQueued {
            status: PrivateAdmissionStatus::Accepted,
            completion: Some(completion),
        });
    tokio::task::yield_now().await;

    // Then: it waits for verification and returns only the aggregate status.
    assert!(!call.is_finished());
    completion_sender
        .send(Ok(()))
        .expect("the RPC is waiting for completion");
    let status = call
        .await
        .expect("the RPC task does not panic")
        .expect("successful verification returns a status");
    assert_eq!(status, PrivateAdmissionStatus::Accepted);
    assert_eq!(
        serde_json::to_string(&status).expect("status serializes"),
        "\"accepted\""
    );
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn exact_retry_returns_existing_without_waiting() {
    // Given: the mempool recognizes an exact private-admission retry.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let call = tokio::spawn(async move { rpc.send_private_transaction(raw, ADMISSION_ID).await });
    mempool
        .expect_request(mempool::Request::QueuePrivate {
            transaction,
            context: context(),
        })
        .await
        .respond(mempool::Response::PrivateQueued {
            status: PrivateAdmissionStatus::Existing,
            completion: None,
        });

    // When: the response is delivered without a completion receiver.
    let status = call
        .await
        .expect("the RPC task does not panic")
        .expect("an exact retry succeeds");

    // Then: it immediately returns only Existing and sends no extra request.
    assert_eq!(status, PrivateAdmissionStatus::Existing);
    assert_eq!(
        serde_json::to_string(&status).expect("status serializes"),
        "\"existing\""
    );
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn invalid_private_transaction_never_calls_mempool() {
    // Given: malformed hex and non-transaction bytes.
    let (mut mempool, rpc, queue_task) = test_rpc();

    // When: each value crosses the RPC parsing boundary.
    let malformed = rpc
        .send_private_transaction("private-plaintext".to_owned(), ADMISSION_ID)
        .await;
    let invalid_transaction = rpc
        .send_private_transaction("00".to_owned(), ADMISSION_ID)
        .await;

    // Then: both use legacy deserialization errors without echoing input or identity.
    for (result, secret) in [
        (malformed, "private-plaintext"),
        (invalid_transaction, "9223372036854775001"),
    ] {
        let error = result.expect_err("invalid input is rejected");
        assert_eq!(error.code(), ErrorCode::ServerError(-22).code());
        let serialized = serde_json::to_string(&error).expect("error serializes");
        assert!(!serialized.contains(secret));
    }
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn accepted_verification_failure_uses_legacy_verify_error() {
    // Given: an accepted submission whose verifier rejects it.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let raw_boundary = raw.clone();
    let (completion_sender, completion) = tokio::sync::oneshot::channel();
    let call = tokio::spawn(async move { rpc.send_private_transaction(raw, ADMISSION_ID).await });
    mempool
        .expect_request(mempool::Request::QueuePrivate {
            transaction,
            context: context(),
        })
        .await
        .respond(mempool::Response::PrivateQueued {
            status: PrivateAdmissionStatus::Accepted,
            completion: Some(completion),
        });

    // When: verification completes with an opaque failure.
    completion_sender
        .send(Err("private verification failed".into()))
        .expect("the RPC is waiting for completion");
    let error = call
        .await
        .expect("the RPC task does not panic")
        .expect_err("verification failure reaches the caller");

    // Then: it uses -25 and does not add request identifiers, hashes, bytes, or timing.
    assert_eq!(error.code(), ErrorCode::ServerError(-25).code());
    let serialized = serde_json::to_string(&error).expect("error serializes");
    assert!(!serialized.contains("9223372036854775001"));
    assert!(!serialized.contains(&raw_boundary));
    assert!(!serialized.contains("timestamp"));
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn mempool_service_failure_uses_existing_call_service_mapping() {
    // Given: a valid request and a disabled mempool service.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let call = tokio::spawn(async move { rpc.send_private_transaction(raw, ADMISSION_ID).await });
    mempool
        .expect_request(mempool::Request::QueuePrivate {
            transaction,
            context: context(),
        })
        .await
        .respond_error(Box::new(mempool::MempoolDisabledError));

    // When: the service error reaches the shared RPC adapter.
    let error = call
        .await
        .expect("the RPC task does not panic")
        .expect_err("disabled mempool is reported");

    // Then: call_service preserves the existing miscellaneous mapping and redacted shape.
    assert_eq!(error.code(), ErrorCode::ServerError(-1).code());
    assert!(error.data().is_none());
    let serialized = serde_json::to_string(&error).expect("error serializes");
    assert!(!serialized.contains("9223372036854775001"));
    assert!(!serialized.contains("timestamp"));
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}

#[tokio::test]
async fn private_pool_info_maps_exact_aggregate_diagnostics() {
    // Given: aggregate diagnostics with distinct sentinel values.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let diagnostics = PrivatePoolDiagnostics {
        transaction_count: 1,
        serialized_bytes: 2,
        max_transactions: 3,
        max_serialized_bytes: 4,
        embargoed_count: 5,
        eligible_count: 6,
        releasing_count: 7,
        scheduler_state: SchedulerState::Stalled,
        promoted_count: 8,
        recoverable_count: 9,
        terminal_count: 10,
    };
    let call = tokio::spawn(async move { rpc.get_private_pool_info().await });
    mempool
        .expect_request(mempool::Request::PrivatePoolDiagnostics)
        .await
        .respond(mempool::Response::PrivatePoolDiagnostics(diagnostics));

    // When: the diagnostics RPC completes.
    let response = call
        .await
        .expect("the RPC task does not panic")
        .expect("diagnostics succeed");

    // Then: it returns exactly the aggregate service contract and no identity or timing fields.
    assert_eq!(response, diagnostics);
    assert_eq!(
        serde_json::to_value(response).expect("diagnostics serialize"),
        json!({
            "transaction_count": 1,
            "serialized_bytes": 2,
            "max_transactions": 3,
            "max_serialized_bytes": 4,
            "embargoed_count": 5,
            "eligible_count": 6,
            "releasing_count": 7,
            "scheduler_state": "stalled",
            "promoted_count": 8,
            "recoverable_count": 9,
            "terminal_count": 10
        })
    );
    mempool.expect_no_requests().await;
    assert!(queue_task.now_or_never().is_none());
}
