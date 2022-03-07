use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use compactor::{handler::CompactorHandler, server::CompactorServer};
use hyper::{Body, Request, Response};
use metric::Registry;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use crate::{
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::{add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{common_state::CommonServerState, RpcError, ServerType},
};

#[derive(Debug)]
pub struct CompactorServerType<C: CompactorHandler> {
    server: CompactorServer<C>,
    shutdown: CancellationToken,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<C: CompactorHandler> CompactorServerType<C> {
    pub fn new(server: CompactorServer<C>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<C: CompactorHandler + std::fmt::Debug + 'static> ServerType for CompactorServerType<C> {
    type RouteError = IoxHttpError;

    /// Return the [`metric::Registry`] used by the compactor.
    fn metric_registry(&self) -> Arc<Registry> {
        self.server.metric_registry()
    }

    /// Returns the trace collector for compactor traces.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.as_ref().map(Arc::clone)
    }

    /// Just return "not found".
    async fn route_http_request(
        &self,
        _req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        Err(IoxHttpError::NotFound)
    }

    /// Provide a placeholder gRPC service.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        // TODO add a service here
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Simple error struct, we're not really providing an HTTP interface for the compactor.
#[derive(Debug)]
pub enum IoxHttpError {
    NotFound,
}

impl IoxHttpError {
    fn status_code(&self) -> HttpApiErrorCode {
        match self {
            IoxHttpError::NotFound => HttpApiErrorCode::NotFound,
        }
    }
}

impl Display for IoxHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for IoxHttpError {}

impl HttpApiErrorSource for IoxHttpError {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.status_code(), self.to_string())
    }
}