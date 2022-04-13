/// CLI config for Kafka partition information
#[derive(Debug, Clone, clap::Parser)]
pub struct KafkaPartitionConfig {
    /// DEPRECATED: Write buffer partition number to start (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START"
    )]
    write_buffer_partition_range_start: Option<i32>,

    /// DEPRECATED: Write buffer partition number to end (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END"
    )]
    write_buffer_partition_range_end: Option<i32>,

    /// First write buffer kafka partition number (inclusive) this
    /// service is responsible for
    #[clap(
        long = "--write-buffer-kafka-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_KAFKA_PARTITION_RANGE_START"
    )]
    write_buffer_kafka_partition_range_start: Option<i32>,

    /// Last write buffer kafka partition number (inclusive) this
    /// service is responsible for
    #[clap(
        long = "--write-buffer-kafka-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_KAFKA_PARTITION_RANGE_END"
    )]
    write_buffer_kafka_partition_range_end: Option<i32>,
}

impl KafkaPartitionConfig {
    /// Return the first write buffer kafka partition number (inclusive) this
    /// ingester is responsible for
    pub fn range_start(&self) -> i32 {
        match (
            self.write_buffer_partition_range_start,
            self.write_buffer_kafka_partition_range_start,
        ) {
            // this code is temporary as we roll out the change, so don't worry about nice error handling
            // https://github.com/influxdata/influxdb_iox/issues/4311
            (Some(_), Some(_)) => panic!(
                "Can not specify both write_buffer_partition_range_start and \
                                          write_buffer_kafka_partition_range_start (partition)"
            ),
            (Some(v), None) => v,
            (None, Some(v)) => v,
            (None, None) => panic!(
                "must specify either write_buffer_partition_range_start or \
                                    write_buffer_kafka_partition_range_start (partition)"
            ),
        }
    }

    /// Return the last write buffer kafka partition number (inclusive) this
    /// ingester is responsible for
    pub fn range_end(&self) -> i32 {
        match (
            self.write_buffer_partition_range_end,
            self.write_buffer_kafka_partition_range_end,
        ) {
            // this code is temporary as we roll out the change, so don't worry about nice error handling
            // https://github.com/influxdata/influxdb_iox/issues/4311
            (Some(_), Some(_)) => panic!(
                "Can not specify both write_buffer_partition_range_end and \
                                          write_buffer_kafka_partition_range_end (partition)"
            ),
            (Some(v), None) => v,
            (None, Some(v)) => v,
            (None, None) => panic!(
                "must specify either write_buffer_partition_range_end or \
                                    write_buffer_kafka_partition_range_end (partition)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::StructOpt;

    use super::*;

    #[test]
    fn test_values() {
        let cfg = KafkaPartitionConfig::try_parse_from([
            "my_binary",
            "--write-buffer-kafka-partition-range-start",
            "1",
            "--write-buffer-kafka-partition-range-end",
            "42",
        ])
        .unwrap();
        assert_eq!(cfg.range_start(), 1);
        assert_eq!(cfg.range_end(), 42);
    }

    #[test]
    fn test_legacy_values() {
        let cfg = KafkaPartitionConfig::try_parse_from([
            "my_binary",
            // note doesn't have 'partition'
            "--write-buffer-partition-range-start",
            "1",
            "--write-buffer-partition-range-end",
            "42",
        ])
        .unwrap();
        assert_eq!(cfg.range_start(), 1);
        assert_eq!(cfg.range_end(), 42);
    }

    #[test]
    #[should_panic(
        expected = "Can not specify both write_buffer_partition_range_start and write_buffer_kafka_partition_range_start (partition)"
    )]
    fn test_duplicated_start() {
        let cfg = KafkaPartitionConfig::try_parse_from([
            "my_binary",
            "--write-buffer-partition-range-start",
            "1",
            // duplicated
            "--write-buffer-kafka-partition-range-start",
            "1",
            "--write-buffer-partition-range-end",
            "42",
        ])
        .unwrap();

        cfg.range_start();
    }

    #[test]
    #[should_panic(
        expected = "Can not specify both write_buffer_partition_range_end and write_buffer_kafka_partition_range_end (partition)"
    )]
    fn test_duplicated_end() {
        let cfg = KafkaPartitionConfig::try_parse_from([
            "my_binary",
            "--write-buffer-partition-range-start",
            "1",
            "--write-buffer-partition-range-end",
            "42",
            // duplicated
            "--write-buffer-kafka-partition-range-end",
            "42",
        ])
        .unwrap();

        cfg.range_end();
    }
}
