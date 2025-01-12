use influxdb_iox_client::{
    error::Error,
    flight::generated_types::ReadInfo,
    router::generated_types::{
        write_sink, HashRing, Matcher, MatcherToShard, Router, ShardConfig, WriteSink, WriteSinkSet,
    },
};
use test_helpers::assert_contains;

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::DatabaseBuilder,
};

use super::scenario::{create_readable_database, rand_name};
use arrow_util::assert_batches_sorted_eq;
use std::{collections::HashMap, num::NonZeroU32};

#[tokio::test]
async fn test_write() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut write_client = fixture.write_client();

    // need a database that is easy to get into the hard buffer limit:
    // 1. turn persist on so we cannot drop unpersisted data
    // 2. set mutable buffer threshold high so IOx will keep the MUB growing
    // 3. use small buffer limits to speed up the test
    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .mub_row_threshold(1_000_000)
        .buffer_size_soft(100_000)
        .buffer_size_hard(200_000)
        .build(fixture.grpc_channel())
        .await;

    // ---- test successful writes ----
    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    let num_lines_written = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 3);

    // ---- test bad data ----
    let err = write_client
        .write_lp(&db_name, "XXX", 0)
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"error parsing line 1: A generic parsing error occurred: TakeWhile1"#
    );
    assert!(matches!(err, Error::Client(_)));

    // ---- test non existent database ----
    let err = write_client
        .write_lp("Non_existent_database", lp_lines.join("\n"), 0)
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"Some requested entity was not found: Resource database/Non_existent_database not found"#
    );
    assert!(matches!(dbg!(err), Error::NotFound(_)));

    // ---- test hard limit ----
    // stream data until limit is reached
    let mut maybe_err = None;
    for i in 0..1_000 {
        let lp_lines: Vec<_> = (0..1_000)
            .map(|j| format!("flood,tag1={},tag2={} x={},y={} 0", i, j, i, j))
            .collect();
        if let Err(err) = write_client
            .write_lp(&db_name, lp_lines.join("\n"), 0)
            .await
        {
            maybe_err = Some(err);
            break;
        }
    }
    assert!(maybe_err.is_some());
    let err = maybe_err.unwrap();
    assert!(matches!(err, Error::ResourceExhausted(_)));

    // IMPORTANT: At this point, the database is flooded and pretty much
    // useless. Don't append any tests after the "hard limit" test!
}

#[tokio::test]
async fn test_write_routed() {
    let test_router_id = NonZeroU32::new(1).unwrap();

    let test_target_id_1 = NonZeroU32::new(2).unwrap();
    let test_target_id_2 = NonZeroU32::new(3).unwrap();
    let test_target_id_3 = NonZeroU32::new(4).unwrap();

    const TEST_REMOTE_ID_1: u32 = 2;
    const TEST_REMOTE_ID_2: u32 = 3;
    const TEST_REMOTE_ID_3: u32 = 4;

    const TEST_SHARD_ID_1: u32 = 42;
    const TEST_SHARD_ID_2: u32 = 43;
    const TEST_SHARD_ID_3: u32 = 44;

    let router = ServerFixture::create_single_use(ServerType::Router).await;
    let mut router_deployment = router.deployment_client();
    let mut router_remote = router.remote_client();
    let mut router_router = router.router_client();
    router_deployment
        .update_server_id(test_router_id)
        .await
        .expect("set ID failed");

    let target_1 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_1_deployment = target_1.deployment_client();
    target_1_deployment
        .update_server_id(test_target_id_1)
        .await
        .expect("set ID failed");
    target_1.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_1, target_1.grpc_base())
        .await
        .expect("set remote failed");

    let target_2 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_2_deployment = target_2.deployment_client();
    target_2_deployment
        .update_server_id(test_target_id_2)
        .await
        .expect("set ID failed");
    target_2.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_2, target_2.grpc_base())
        .await
        .expect("set remote failed");

    let target_3 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_3_deployment = target_3.deployment_client();
    target_3_deployment
        .update_server_id(test_target_id_3)
        .await
        .expect("set ID failed");
    target_3.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_3, target_3.grpc_base())
        .await
        .expect("set remote failed");

    let db_name = rand_name();
    create_readable_database(&db_name, target_1.grpc_channel()).await;
    create_readable_database(&db_name, target_2.grpc_channel()).await;
    create_readable_database(&db_name, target_3.grpc_channel()).await;

    // Set sharding rules on the router:
    let router_config = Router {
        name: db_name.clone(),
        write_sharder: Some(ShardConfig {
            specific_targets: vec![
                MatcherToShard {
                    matcher: Some(Matcher {
                        table_name_regex: "^cpu$".to_string(),
                    }),
                    shard: TEST_SHARD_ID_1,
                },
                MatcherToShard {
                    matcher: Some(Matcher {
                        table_name_regex: "^mem$".to_string(),
                    }),
                    shard: TEST_SHARD_ID_3,
                },
            ],
            hash_ring: Some(HashRing {
                shards: vec![TEST_SHARD_ID_2],
            }),
        }),
        write_sinks: HashMap::from([
            (
                TEST_SHARD_ID_1,
                WriteSinkSet {
                    sinks: vec![WriteSink {
                        sink: Some(write_sink::Sink::GrpcRemote(TEST_REMOTE_ID_1)),
                        ignore_errors: false,
                    }],
                },
            ),
            (
                TEST_SHARD_ID_2,
                WriteSinkSet {
                    sinks: vec![WriteSink {
                        sink: Some(write_sink::Sink::GrpcRemote(TEST_REMOTE_ID_2)),
                        ignore_errors: false,
                    }],
                },
            ),
            (
                TEST_SHARD_ID_3,
                WriteSinkSet {
                    sinks: vec![WriteSink {
                        sink: Some(write_sink::Sink::GrpcRemote(TEST_REMOTE_ID_3)),
                        ignore_errors: false,
                    }],
                },
            ),
        ]),
        query_sinks: None,
    };
    router_router
        .update_router(router_config)
        .await
        .expect("cannot update router rules");

    // Write some data
    let mut write_client = router.write_client();

    let lp_lines = vec![
        "cpu bar=1 100",
        "cpu bar=2 200",
        "disk bar=3 300",
        "mem baz=4 400",
    ];

    let num_lines_written = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 4);

    // The router will have split the write request by table name into two shards.
    // Target 1 will have received only the "cpu" table.
    // Target 2 will have received only the "disk" table.

    let mut query_results = target_1
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name.clone(),
            sql_query: "select * from cpu".to_string(),
        })
        .await
        .expect("failed to query target 1");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 1   | 1970-01-01T00:00:00.000000100Z |",
        "| 2   | 1970-01-01T00:00:00.000000200Z |",
        "+-----+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_1
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name.clone(),
            sql_query: "select * from disk".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));

    let mut query_results = target_2
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name.clone(),
            sql_query: "select * from disk".to_string(),
        })
        .await
        .expect("failed to query target 2");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 3   | 1970-01-01T00:00:00.000000300Z |",
        "+-----+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_2
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name.clone(),
            sql_query: "select * from cpu".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    ////

    let mut query_results = target_3
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name,
            sql_query: "select * from mem".to_string(),
        })
        .await
        .expect("failed to query target 3");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+--------------------------------+",
        "| baz | time                           |",
        "+-----+--------------------------------+",
        "| 4   | 1970-01-01T00:00:00.000000400Z |",
        "+-----+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}

#[tokio::test]
async fn test_write_routed_errors() {
    let test_router_id = NonZeroU32::new(1).unwrap();
    const TEST_REMOTE_ID: u32 = 2;
    const TEST_SHARD_ID: u32 = 42;

    let router = ServerFixture::create_single_use(ServerType::Router).await;
    let mut router_deployment = router.deployment_client();
    let mut router_router = router.router_client();
    router_deployment
        .update_server_id(test_router_id)
        .await
        .expect("set ID failed");

    let db_name = rand_name();

    // Set sharding rules on the router:
    let router_config = Router {
        name: db_name.clone(),
        write_sharder: Some(ShardConfig {
            specific_targets: vec![MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: "^cpu$".to_string(),
                }),
                shard: TEST_SHARD_ID,
            }],
            hash_ring: None,
        }),
        write_sinks: HashMap::from([(
            TEST_SHARD_ID,
            WriteSinkSet {
                sinks: vec![WriteSink {
                    sink: Some(write_sink::Sink::GrpcRemote(TEST_REMOTE_ID)),
                    ignore_errors: false,
                }],
            },
        )]),
        query_sinks: None,
    };
    router_router
        .update_router(router_config)
        .await
        .expect("cannot update router rules");

    // We intentionally omit to configure the "remotes" name resolution on the
    // router server.
    let mut write_client = router.write_client();
    let lp_lines = vec!["cpu bar=1 100", "cpu bar=2 200"];
    let err = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        format!(
            "The operation was aborted: One or more writes failed: \
            ShardId({}) => \"Write to sink set failed: No remote for server ID {}\"",
            TEST_SHARD_ID, TEST_REMOTE_ID,
        )
    );

    // TODO(mkm): check connection error and successful communication with a
    // target that replies with an error...
}

#[tokio::test]
async fn test_write_dev_null() {
    let test_router_id = NonZeroU32::new(1).unwrap();
    const TEST_SHARD_ID: u32 = 42;

    let router = ServerFixture::create_single_use(ServerType::Router).await;
    let mut router_deployment = router.deployment_client();
    let mut router_router = router.router_client();
    router_deployment
        .update_server_id(test_router_id)
        .await
        .expect("set ID failed");

    let db_name = rand_name();

    // Set sharding rules on the router:
    let router_config = Router {
        name: db_name.clone(),
        write_sharder: Some(ShardConfig {
            specific_targets: vec![MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: "^cpu$".to_string(),
                }),
                shard: TEST_SHARD_ID,
            }],
            hash_ring: None,
        }),
        write_sinks: HashMap::from([(TEST_SHARD_ID, WriteSinkSet { sinks: vec![] })]),
        query_sinks: None,
    };
    router_router
        .update_router(router_config)
        .await
        .expect("cannot update router rules");

    // Rows matching a shard directed to "/dev/null" are silently ignored
    let mut write_client = router.write_client();
    let lp_lines = vec!["cpu bar=1 100", "cpu bar=2 200"];
    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("dev null eats them all");

    // Rows not matching that shard won't be send to "/dev/null".
    let lp_lines = vec!["mem bar=1 1", "mem bar=2 2"];
    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("no shard means dev null as well");
}

#[tokio::test]
async fn test_write_routed_no_shard() {
    let test_router_id = NonZeroU32::new(1).unwrap();

    let test_target_id_1 = NonZeroU32::new(2).unwrap();
    let test_target_id_2 = NonZeroU32::new(3).unwrap();
    let test_target_id_3 = NonZeroU32::new(4).unwrap();

    const TEST_REMOTE_ID_1: u32 = 2;
    const TEST_REMOTE_ID_2: u32 = 3;
    const TEST_REMOTE_ID_3: u32 = 4;

    const TEST_SHARD_ID: u32 = 42;

    let router = ServerFixture::create_single_use(ServerType::Router).await;
    let mut router_deployment = router.deployment_client();
    let mut router_remote = router.remote_client();
    let mut router_router = router.router_client();
    router_deployment
        .update_server_id(test_router_id)
        .await
        .expect("set ID failed");

    let target_1 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_1_deployment = target_1.deployment_client();
    target_1_deployment
        .update_server_id(test_target_id_1)
        .await
        .expect("set ID failed");
    target_1.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_1, target_1.grpc_base())
        .await
        .expect("set remote failed");

    let target_2 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_2_deployment = target_2.deployment_client();
    target_2_deployment
        .update_server_id(test_target_id_2)
        .await
        .expect("set ID failed");
    target_2.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_2, target_2.grpc_base())
        .await
        .expect("set remote failed");

    let target_3 = ServerFixture::create_single_use(ServerType::Database).await;
    let mut target_3_deployment = target_3.deployment_client();
    target_3_deployment
        .update_server_id(test_target_id_3)
        .await
        .expect("set ID failed");
    target_3.wait_server_initialized().await;

    router_remote
        .update_remote(TEST_REMOTE_ID_3, target_3.grpc_base())
        .await
        .expect("set remote failed");

    let db_name_1 = rand_name();
    let db_name_2 = rand_name();
    for &db_name in &[&db_name_1, &db_name_2] {
        create_readable_database(db_name, target_1.grpc_channel()).await;
        create_readable_database(db_name, target_2.grpc_channel()).await;
        create_readable_database(db_name, target_3.grpc_channel()).await;
    }

    // Set routing rules on the router:
    for (db_name, remote_id) in &[
        (db_name_1.clone(), TEST_REMOTE_ID_1),
        (db_name_2.clone(), TEST_REMOTE_ID_2),
    ] {
        let router_config = Router {
            name: db_name.clone(),
            write_sharder: Some(ShardConfig {
                specific_targets: vec![MatcherToShard {
                    matcher: Some(Matcher {
                        table_name_regex: ".*".to_string(),
                    }),
                    shard: TEST_SHARD_ID,
                }],
                hash_ring: None,
            }),
            write_sinks: HashMap::from([(
                TEST_SHARD_ID,
                WriteSinkSet {
                    sinks: vec![WriteSink {
                        sink: Some(write_sink::Sink::GrpcRemote(*remote_id)),
                        ignore_errors: false,
                    }],
                },
            )]),
            query_sinks: None,
        };
        router_router
            .update_router(router_config)
            .await
            .expect("cannot update router rules");
    }

    // Write some data
    let line_1 = "cpu bar=1 100";
    let line_2 = "disk bar=2 100";

    let mut write_client = router.write_client();

    for (&ref db_name, &ref line) in &[(&db_name_1, line_1), (&db_name_2, line_2)] {
        let num_lines_written = write_client
            .write_lp(db_name, line, 0)
            .await
            .expect("cannot write");
        assert_eq!(num_lines_written, 1);
    }

    // The router will have split the write request by database name.
    // Target 1 will have received only the "cpu" table.
    // Target 2 will have received only the "disk" table.
    // Target 3 won't get any writes.

    let mut query_results = target_1
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_1.clone(),
            sql_query: "select * from cpu".to_string(),
        })
        .await
        .expect("failed to query target 1");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 1   | 1970-01-01T00:00:00.000000100Z |",
        "+-----+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_1
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_1.clone(),
            sql_query: "select * from disk".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));

    let mut query_results = target_2
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_2.clone(),
            sql_query: "select * from disk".to_string(),
        })
        .await
        .expect("failed to query target 2");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 2   | 1970-01-01T00:00:00.000000100Z |",
        "+-----+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_2
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_1.clone(),
            sql_query: "select * from cpu".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    // Ensure that target_3 didn't get any writes.

    assert!(target_3
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_1,
            sql_query: "select * from cpu".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    assert!(target_3
        .flight_client()
        .perform_query(ReadInfo {
            namespace_name: db_name_2,
            sql_query: "select * from disk".to_string()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));
}

#[tokio::test]
async fn test_write_schema_mismatch() {
    // regression test for https://github.com/influxdata/influxdb_iox/issues/2538
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    write_client
        .write_lp(&db_name, "table field=1i 10", 0)
        .await
        .expect("cannot write");

    let err = write_client
        .write_lp(&db_name, "table field=1.1 10", 0)
        .await
        .unwrap_err();
    assert_contains!(err.to_string(), "Schema Merge Error");
    assert!(matches!(err, Error::InvalidArgument(_)));
}
