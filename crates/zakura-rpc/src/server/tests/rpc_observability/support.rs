use std::{
    borrow::Cow,
    fmt::Debug,
    future::{ready, Ready},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use jsonrpsee::{
    server::middleware::rpc::RpcServiceT,
    types::{ErrorCode, Id, Request},
    MethodResponse, ResponsePayload,
};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use serde_json::value::RawValue;
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id as SpanId, Record},
    Dispatch, Event, Metadata as TracingMetadata, Subscriber,
};

use crate::server::{
    rpc_logger::RpcLoggerMiddleware, rpc_metrics::RpcMetricsMiddleware,
    rpc_tracing::RpcTracingMiddleware,
};

pub(super) const INVALID_PAYLOAD: &str = "INVALID_PRIVATE_PAYLOAD_SENTINEL";
pub(super) const ADMISSION_ID: &str = "ADMISSION_ID_SENTINEL";

#[derive(Clone, Copy)]
enum MockResponse {
    Success,
    Error,
}

#[derive(Clone)]
struct MockRpcService(MockResponse);

impl<'a> RpcServiceT<'a> for MockRpcService {
    type Future = Ready<MethodResponse>;

    fn call(&self, request: Request<'a>) -> Self::Future {
        let response = match self.0 {
            MockResponse::Success => MethodResponse::response(
                request.id(),
                ResponsePayload::success(ADMISSION_ID),
                usize::MAX,
            ),
            MockResponse::Error => MethodResponse::error(request.id(), ErrorCode::InvalidParams),
        };

        ready(response)
    }
}

#[derive(Default)]
struct CapturedTracing {
    observations: Arc<Mutex<Vec<String>>>,
    next_span_id: AtomicU64,
}

impl CapturedTracing {
    fn observations(&self) -> Arc<Mutex<Vec<String>>> {
        self.observations.clone()
    }

    fn push(&self, prefix: &str, visitor: FieldVisitor) {
        self.observations
            .lock()
            .expect("capture lock should not be poisoned")
            .push(format!("{prefix} {}", visitor.values.join(" ")));
    }
}

impl Subscriber for CapturedTracing {
    fn enabled(&self, _metadata: &TracingMetadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> SpanId {
        let mut visitor = FieldVisitor::default();
        span.record(&mut visitor);
        self.push(span.metadata().name(), visitor);
        SpanId::from_u64(self.next_span_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _span: &SpanId, values: &Record<'_>) {
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        self.push("record", visitor);
    }

    fn record_follows_from(&self, _span: &SpanId, _follows: &SpanId) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.push(event.metadata().target(), visitor);
    }

    fn enter(&self, _span: &SpanId) {}

    fn exit(&self, _span: &SpanId) {}
}

#[derive(Default)]
struct FieldVisitor {
    values: Vec<String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.values.push(format!("{}={value:?}", field.name()));
    }
}

#[derive(Default)]
struct CapturedMetrics(Mutex<Vec<String>>);

impl CapturedMetrics {
    fn record_key(&self, key: &Key) {
        let labels = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect::<Vec<_>>()
            .join(",");
        self.0
            .lock()
            .expect("metrics lock should not be poisoned")
            .push(format!("{}{{{labels}}}", key.name()));
    }

    fn observations(&self) -> Vec<String> {
        self.0
            .lock()
            .expect("metrics lock should not be poisoned")
            .clone()
    }
}

impl Recorder for CapturedMetrics {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.record_key(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.record_key(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.record_key(key);
        Histogram::noop()
    }
}

fn request(method: &'static str) -> (Box<RawValue>, Request<'static>) {
    let params = RawValue::from_string(format!(r#"["{INVALID_PAYLOAD}"]"#))
        .expect("synthetic parameters should be valid JSON");
    let request = Request::new(Cow::Borrowed(method), None, Id::Number(1));
    (params, request)
}

pub(super) async fn observe_call(method: &'static str) -> (Vec<String>, Vec<String>) {
    let tracing = CapturedTracing::default();
    let observations = tracing.observations();
    let dispatch = Dispatch::new(tracing);
    let metrics = CapturedMetrics::default();
    let _metrics_guard = metrics::set_default_local_recorder(&metrics);
    let _tracing_guard = tracing::dispatcher::set_default(&dispatch);

    let (params, mut logger_request) = request(method);
    logger_request.params = Some(Cow::Owned(params));
    RpcLoggerMiddleware::new(MockRpcService(MockResponse::Success), 1024)
        .call(logger_request)
        .await;

    RpcTracingMiddleware::new(MockRpcService(MockResponse::Error))
        .call(request(method).1)
        .await;
    RpcMetricsMiddleware::new(MockRpcService(MockResponse::Error))
        .call(request(method).1)
        .await;

    let tracing = observations
        .lock()
        .expect("capture lock should not be poisoned")
        .clone();
    (tracing, metrics.observations())
}
