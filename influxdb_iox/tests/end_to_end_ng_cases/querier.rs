use test_helpers_end_to_end_ng::{
    maybe_skip_integration, write_read::WriteReadTest, MiniCluster, TestConfig,
};

#[tokio::test]
async fn basic_ingester() {
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router2_config = TestConfig::new_router2(&database_url);
    let ingester_config = TestConfig::new_ingester(&router2_config);
    let querier_config = TestConfig::new_querier(&ingester_config);

    // Set up the cluster  ====================================
    let cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    WriteReadTest::new(&cluster)
        .with_line_protocol(format!(
            "{},tag1=A,tag2=B val=42i 123456\n\
             {},tag1=A,tag2=C val=43i 123457",
            table_name, table_name
        ))
        .with_query(format!("select * from {}", table_name))
        .with_expected([
            "+------+------+--------------------------------+-----+",
            "| tag1 | tag2 | time                           | val |",
            "+------+------+--------------------------------+-----+",
            "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
            "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
            "+------+------+--------------------------------+-----+",
        ])
        .run()
        .await
}

#[tokio::test]
async fn basic_on_parquet() {
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router2_config = TestConfig::new_router2(&database_url);
    // fast parquet
    let ingester_config = TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
    let querier_config = TestConfig::new_querier(&ingester_config);

    // Set up the cluster  ====================================
    let cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    WriteReadTest::new(&cluster)
        .with_line_protocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name))
        // Wait for data to be persisted to parquet
        .with_wait_for_parquet()
        .with_query(format!("select * from {}", table_name))
        .with_expected([
            "+------+------+--------------------------------+-----+",
            "| tag1 | tag2 | time                           | val |",
            "+------+------+--------------------------------+-----+",
            "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
            "+------+------+--------------------------------+-----+",
        ])
        .run()
        .await
}

#[tokio::test]
async fn basic_no_ingster_connection() {
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router2_config = TestConfig::new_router2(&database_url);
    // fast parquet
    let ingester_config = TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();

    // specially create a querier config that is NOT connected to the ingester
    let querier_config = TestConfig::new_querier_without_ingester(&ingester_config);

    // Set up the cluster  ====================================
    let cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    // Write some data into the v2 HTTP API ==============
    WriteReadTest::new(&cluster)
        .with_line_protocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name))
        // Wait for data to be persisted to parquet
        .with_wait_for_parquet()
        .with_query(format!("select * from {}", table_name))
        .with_expected([
            "+------+------+--------------------------------+-----+",
            "| tag1 | tag2 | time                           | val |",
            "+------+------+--------------------------------+-----+",
            "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
            "+------+------+--------------------------------+-----+",
        ])
        .run()
        .await
}
