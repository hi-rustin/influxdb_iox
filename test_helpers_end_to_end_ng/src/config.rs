use std::{collections::HashMap, sync::Arc};

use http::{header::HeaderName, HeaderValue};
use rand::Rng;
use tempfile::TempDir;

use crate::addrs::BindAddresses;

use super::ServerType;

/// Options for creating test servers (`influxdb_iox` processes)
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// environment variables to pass to server process. HashMap to avoid duplication
    env: HashMap<String, String>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,

    /// Server type
    server_type: ServerType,

    /// Catalog DSN value
    dsn: String,

    /// Catalog schema name
    catalog_schema_name: String,

    /// Write buffer directory, if needed
    write_buffer_dir: Option<Arc<TempDir>>,

    /// Object store directory, if needed.
    object_store_dir: Option<Arc<TempDir>>,

    /// Which ports this server should use
    addrs: Arc<BindAddresses>,
}

impl TestConfig {
    /// Create a new TestConfig (tests should use one of the specific
    /// configuration setup below, such as [new_router2]
    fn new(
        server_type: ServerType,
        dsn: impl Into<String>,
        catalog_schema_name: impl Into<String>,
    ) -> Self {
        Self {
            env: HashMap::new(),
            client_headers: vec![],
            server_type,
            dsn: dsn.into(),
            catalog_schema_name: catalog_schema_name.into(),
            write_buffer_dir: None,
            object_store_dir: None,
            addrs: Arc::new(BindAddresses::default()),
        }
    }

    /// Create a minimal router2 configuration
    pub fn new_router2(dsn: impl Into<String>) -> Self {
        Self::new(ServerType::Router2, dsn, random_catalog_schema_name())
            .with_new_write_buffer()
            .with_new_object_store()
    }

    /// Create a minimal ingester configuration, using the dsn and
    /// write buffer configuration from other
    pub fn new_ingester(other: &TestConfig) -> Self {
        Self::new(
            ServerType::Ingester,
            other.dsn(),
            other.catalog_schema_name(),
        )
        .with_existing_write_buffer(other)
        .with_existing_object_store(other)
        .with_default_ingester_options()
    }

    /// Create a minimal querier configuration from the specified
    /// ingester configuration, using the same dsn and object store,
    /// and pointing at the specified ingester
    pub fn new_querier(ingester_config: &TestConfig) -> Self {
        assert_eq!(ingester_config.server_type(), ServerType::Ingester);

        let ingester_address =
            Arc::clone(&ingester_config.addrs().ingester_grpc_api().client_base());

        Self::new_querier_without_ingester(ingester_config)
            // Configure to talk with the ingester
            .with_ingester_addresses(&[ingester_address.as_ref()])
    }

    /// Create a minimal querier configuration from the specified
    /// ingester configuration, using the same dsn and object store
    pub fn new_querier_without_ingester(ingester_config: &TestConfig) -> Self {
        Self::new(
            ServerType::Querier,
            ingester_config.dsn(),
            ingester_config.catalog_schema_name(),
        )
        .with_existing_object_store(ingester_config)
    }

    /// Create a minimal all in one configuration
    pub fn new_all_in_one(dsn: impl Into<String>) -> Self {
        Self::new(ServerType::AllInOne, dsn, random_catalog_schema_name())
            .with_new_write_buffer()
            .with_new_object_store()
            .with_default_ingester_options()
            .with_fast_parquet_generation()
    }

    /// Create a configuraton for aggressive creation of parquet files
    pub fn with_fast_parquet_generation(self) -> Self {
        self.with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "2")
            .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "1")
    }

    // Get the catalog DSN URL and panic if it's not set
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    // Get the catalog postgres schema name
    pub fn catalog_schema_name(&self) -> &str {
        &self.catalog_schema_name
    }

    /// Adds default ingester options
    fn with_default_ingester_options(self) -> Self {
        self.with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "20")
            .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "10")
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START", "0")
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END", "0")
    }

    /// Adds the ingester addresses
    fn with_ingester_addresses(self, ingester_addresses: &[&str]) -> Self {
        self.with_env(
            "INFLUXDB_IOX_INGESTER_ADDRESSES",
            ingester_addresses.join(","),
        )
    }

    /// add a name=value environment variable when starting the server
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    /// copy the specified environment variables from other; Panic's if they do not exist.
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn copy_env(self, name: impl Into<String>, other: &TestConfig) -> Self {
        let name = name.into();
        let value = match other.env.get(&name) {
            Some(v) => v.clone(),
            None => panic!(
                "Can not copy {} from existing config. Available values are: {:#?}",
                name, other.env
            ),
        };

        self.with_env(name, value)
    }

    /// Configures a new write buffer
    pub fn with_new_write_buffer(mut self) -> Self {
        let n_sequencers = 1;
        let tmpdir = TempDir::new().expect("can not create tmp dir");
        let write_buffer_string = tmpdir.path().display().to_string();
        self.write_buffer_dir = Some(Arc::new(tmpdir));

        self.with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
            .with_env(
                "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS",
                n_sequencers.to_string(),
            )
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", &write_buffer_string)
    }

    /// Configures this TestConfig to use the same write buffer as other
    pub fn with_existing_write_buffer(mut self, other: &TestConfig) -> Self {
        // copy the the directory, if any
        self.write_buffer_dir = other.write_buffer_dir.clone();
        self.copy_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", other)
            .copy_env("INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS", other)
            .copy_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", other)
    }

    // add a name=value http header to all client requests made to the server
    pub fn with_client_header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.client_headers.push((
            name.as_ref().parse().expect("valid header name"),
            value.as_ref().parse().expect("valid header value"),
        ));
        self
    }

    /// Configures a new objct store
    pub fn with_new_object_store(mut self) -> Self {
        let tmpdir = TempDir::new().expect("can not create tmp dir");

        let object_store_string = tmpdir.path().display().to_string();
        self.object_store_dir = Some(Arc::new(tmpdir));
        self.with_env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .with_env("INFLUXDB_IOX_DB_DIR", &object_store_string)
    }

    /// Configures this TestConfig to use the same object store as other
    pub fn with_existing_object_store(mut self, other: &TestConfig) -> Self {
        // copy a reference to the temp dir, if any
        self.object_store_dir = other.object_store_dir.clone();
        self.copy_env("INFLUXDB_IOX_OBJECT_STORE", other)
            .copy_env("INFLUXDB_IOX_DB_DIR", other)
    }

    /// Get the test config's server type.
    #[must_use]
    pub fn server_type(&self) -> ServerType {
        self.server_type
    }

    /// Get a reference to the test config's env.
    pub fn env(&self) -> impl Iterator<Item = (&str, &str)> {
        self.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Get a reference to the test config's client headers.
    #[must_use]
    pub fn client_headers(&self) -> &[(HeaderName, HeaderValue)] {
        self.client_headers.as_ref()
    }

    /// Get a reference to the test config's addrs.
    #[must_use]
    pub fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }
}

fn random_catalog_schema_name() -> String {
    let mut rng = rand::thread_rng();

    (&mut rng)
        .sample_iter(rand::distributions::Alphanumeric)
        .filter(|c| c.is_ascii_alphabetic())
        .take(20)
        .map(char::from)
        .collect::<String>()
}
