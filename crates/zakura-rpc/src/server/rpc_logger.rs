use jsonrpsee::{
    core::tracing::server::{rx_log_from_json, tx_log_from_str},
    server::middleware::rpc::{layer::ResponseFuture, RpcServiceT},
    MethodResponse,
};
use tracing::Instrument;

#[cfg(feature = "privacy-admission")]
const PRIVATE_RPC_METHOD: &str = "sendprivatetransaction";

pub(super) fn per_call_observability_enabled(method: &str) -> bool {
    #[cfg(feature = "privacy-admission")]
    {
        method != PRIVATE_RPC_METHOD
    }

    #[cfg(not(feature = "privacy-admission"))]
    {
        let _ = method;
        true
    }
}

#[derive(Clone)]
pub(super) struct RpcLoggerMiddleware<S> {
    service: S,
    max_log_len: u32,
}

impl<S> RpcLoggerMiddleware<S> {
    pub(super) fn new(service: S, max_log_len: u32) -> Self {
        Self {
            service,
            max_log_len,
        }
    }
}

impl<'a, S> RpcServiceT<'a> for RpcLoggerMiddleware<S>
where
    S: RpcServiceT<'a> + Send + Sync + Clone + 'static,
{
    type Future = ResponseFuture<futures::future::BoxFuture<'a, MethodResponse>>;

    fn call(&self, request: jsonrpsee::types::Request<'a>) -> Self::Future {
        let service = self.service.clone();

        if !per_call_observability_enabled(request.method_name()) {
            return ResponseFuture::future(Box::pin(async move { service.call(request).await }));
        }

        let max_log_len = self.max_log_len;
        rx_log_from_json(&request, max_log_len);
        let span = tracing::trace_span!("method_call", method = request.method_name());

        ResponseFuture::future(Box::pin(
            async move {
                let response = service.call(request).await;
                tx_log_from_str(response.as_result(), max_log_len);
                response
            }
            .instrument(span),
        ))
    }
}
