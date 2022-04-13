/// CLI config for Kafka partition information
#[derive(Debug, Clone, clap::Parser)]
pub struct KafkaPartitionConfig {
    /// First write buffer kafka partition number (inclusive) this
    /// service is responsible for
    #[clap(
        long = "--write-buffer-kafka-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_KAFKA_PARTITION_RANGE_START"
    )]
    write_buffer_kafka_partition_range_start: i32,

    /// Last write buffer kafka partition number (inclusive) this
    /// service is responsible for
    #[clap(
        long = "--write-buffer-kafka-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_KAFKA_PARTITION_RANGE_END"
    )]
    write_buffer_kafka_partition_range_end: i32,
}

impl KafkaPartitionConfig {
    /// Return the first write buffer kafka partition number (inclusive) this
    /// ingester is responsible for
    pub fn range_start(&self) -> i32 {
        self.write_buffer_kafka_partition_range_start
    }

    /// Return the last write buffer kafka partition number (inclusive) this
    /// ingester is responsible for
    pub fn range_end(&self) -> i32 {
        self.write_buffer_kafka_partition_range_end
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
}
