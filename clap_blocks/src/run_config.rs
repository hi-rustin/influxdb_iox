use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

use crate::{object_store::ObjectStoreConfig, server_id::ServerIdConfig, socket_addr::SocketAddr};

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// Common config for all `run` commands.
#[derive(Debug, Clone, clap::Parser)]
pub struct RunConfig {
    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// object store config
    #[clap(flatten)]
    pub(crate) server_id_config: ServerIdConfig,

    /// The address on which IOx will serve HTTP API requests.
    #[clap(
    long = "--api-bind",
    env = "INFLUXDB_IOX_BIND_ADDR",
    default_value = DEFAULT_API_BIND_ADDR,
    )]
    pub http_bind_address: SocketAddr,

    /// The address on which IOx will serve Storage gRPC API requests.
    #[clap(
    long = "--grpc-bind",
    env = "INFLUXDB_IOX_GRPC_BIND_ADDR",
    default_value = DEFAULT_GRPC_BIND_ADDR,
    )]
    pub grpc_bind_address: SocketAddr,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "--max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760" // 10 MiB
    )]
    pub max_http_request_size: usize,

    /// object store config
    #[clap(flatten)]
    pub(crate) object_store_config: ObjectStoreConfig,
}

impl RunConfig {
    /// Get a reference to the run config's tracing config.
    pub fn tracing_config(&self) -> &TracingConfig {
        &self.tracing_config
    }

    /// Get a reference to the run config's object store config.
    pub fn object_store_config(&self) -> &ObjectStoreConfig {
        &self.object_store_config
    }

    /// Get a mutable reference to the run config's tracing config.
    pub fn tracing_config_mut(&mut self) -> &mut TracingConfig {
        &mut self.tracing_config
    }

    /// Get a reference to the run config's server id config.
    pub fn server_id_config(&self) -> &ServerIdConfig {
        &self.server_id_config
    }

    /// Get a reference to the run config's logging config.
    pub fn logging_config(&self) -> &LoggingConfig {
        &self.logging_config
    }

    /// Get a mutable reference to the run config's server id config.
    pub fn server_id_config_mut(&mut self) -> &mut ServerIdConfig {
        &mut self.server_id_config
    }

    /// set the http bind address
    pub fn with_http_bind_address(mut self, http_bind_address: SocketAddr) -> Self {
        self.http_bind_address = http_bind_address;
        self
    }

    /// set the grpc bind address
    pub fn with_grpc_bind_address(mut self, grpc_bind_address: SocketAddr) -> Self {
        self.grpc_bind_address = grpc_bind_address;
        self
    }
}
