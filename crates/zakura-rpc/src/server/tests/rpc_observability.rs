mod support;

use support::{observe_call, ADMISSION_ID, INVALID_PAYLOAD};

const PRIVATE_METHOD: &str = "sendprivatetransaction";
const ORDINARY_METHOD: &str = "getinfo";

#[cfg(feature = "privacy-admission")]
#[tokio::test]
async fn private_rpc_emits_no_per_call_observability() {
    let (tracing, metrics) = observe_call(PRIVATE_METHOD).await;

    assert!(
        tracing.is_empty(),
        "private RPC emitted tracing: {tracing:?}"
    );
    assert!(
        metrics.is_empty(),
        "private RPC emitted metrics: {metrics:?}"
    );
}

#[tokio::test]
async fn ordinary_rpc_remains_fully_instrumented() {
    let (tracing, metrics) = observe_call(ORDINARY_METHOD).await;

    assert!(tracing.iter().any(|line| line.contains(INVALID_PAYLOAD)));
    assert!(tracing.iter().any(|line| line.contains(ADMISSION_ID)));
    assert!(tracing.iter().any(|line| line.starts_with("rpc_request ")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.active_requests{")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.requests.total{")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.request.duration_seconds{")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.errors.total{")));
}

#[cfg(feature = "privacy-admission")]
#[tokio::test]
async fn similarly_named_rpc_remains_instrumented() {
    let (tracing, metrics) = observe_call("sendprivatetransactionextra").await;

    assert!(tracing.iter().any(|line| line.contains(INVALID_PAYLOAD)));
    assert!(tracing.iter().any(|line| line.starts_with("rpc_request ")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.requests.total{")));
}

#[cfg(not(feature = "privacy-admission"))]
#[tokio::test]
async fn private_method_name_is_instrumented_when_feature_is_disabled() {
    let (tracing, metrics) = observe_call(PRIVATE_METHOD).await;

    assert!(tracing.iter().any(|line| line.contains(INVALID_PAYLOAD)));
    assert!(tracing.iter().any(|line| line.starts_with("rpc_request ")));
    assert!(metrics
        .iter()
        .any(|line| line.starts_with("rpc.requests.total{")));
}
