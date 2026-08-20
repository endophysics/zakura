use super::*;

#[tokio::test]
async fn json_rpc_boundary_assigns_admission_id_internally() {
    // Given: a generated RPC module and raw transaction without an admission ID.
    let (mut mempool, rpc, queue_task) = test_rpc();
    let (raw, transaction) = transaction_fixture();
    let mut methods = RpcServer::into_rpc(rpc.clone());
    methods
        .merge(PrivateRpcServer::into_rpc(rpc))
        .expect("private RPC method names are unique");
    let request = json!({
        "jsonrpc": "2.0",
        "method": "sendprivatetransaction",
        "params": [raw],
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
