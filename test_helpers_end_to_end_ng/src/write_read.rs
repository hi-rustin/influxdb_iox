use http::StatusCode;

use arrow_util::assert_batches_sorted_eq;

use crate::{get_write_token, run_query, wait_for_persisted, wait_for_readable, MiniCluster};

/// Helper for running a test that:
/// 1. Starts up a MiniCluster,
/// 2. Optionally writes data to it
/// 3. Optionally waits for the data after write
/// 4. Runs a query and compares it to the expected output
pub struct WriteReadTest<'a> {
    cluster: &'a MiniCluster,

    /// data to write
    line_protocol: Option<String>,

    /// should the test harness wait for it to be persisted?
    wait_for_parquet: bool,

    /// SQL Query to run
    query: Option<String>,

    /// results to compare results against
    expected: Option<Vec<String>>,
}

impl<'a> WriteReadTest<'a> {
    /// Create a new ready/write test. Must be configured via
    /// `with_line_protocol`, `with_wait_for_persisted`, `with_query`
    /// and `with_expected` or else will panic
    pub fn new(cluster: &'a MiniCluster) -> Self {
        Self {
            cluster,
            line_protocol: None,
            wait_for_parquet: false,
            query: None,
            expected: None,
        }
    }

    pub fn with_line_protocol(mut self, line_protocol: impl Into<String>) -> Self {
        self.line_protocol = Some(line_protocol.into());
        self
    }

    pub fn with_wait_for_parquet(mut self) -> Self {
        self.wait_for_parquet = true;
        self
    }

    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    pub fn with_expected(mut self, expected: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let expected: Vec<_> = expected.into_iter().map(|s| s.into()).collect();
        self.expected = Some(expected);
        self
    }

    /// run the test.
    pub async fn run(self) {
        let Self {
            cluster,
            line_protocol,
            wait_for_parquet,
            query,
            expected,
        } = self;
        let line_protocol = line_protocol.expect("line protocol not specified");
        let query = query.expect("query not specified");
        let expected = expected.expect("expected results not specified");

        // Write some data into the v2 HTTP API ==============
        let response = cluster.write_to_router(line_protocol).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Wait for data to be readable
        let write_token = get_write_token(&response);
        wait_for_readable(&write_token, cluster.ingester().ingester_grpc_connection()).await;

        // wait for persistence, if requested
        if wait_for_parquet {
            wait_for_persisted(write_token, cluster.ingester().ingester_grpc_connection()).await;
        }

        // run query
        let batches = run_query(
            query,
            cluster.namespace(),
            cluster.querier().querier_grpc_connection(),
        )
        .await;

        // convert String --> str
        let expected: Vec<_> = expected.iter().map(|s| s.as_str()).collect();
        assert_batches_sorted_eq!(&expected, &batches);
    }
}
