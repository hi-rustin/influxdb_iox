use crate::{
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
    storage::Storage,
};
use data_types::{
    partition_metadata::TableSummary,
    timestamp::{TimestampMinMax, TimestampRange},
};
use data_types2::{ParquetFile, ParquetFileWithMetadata};
use datafusion::physical_plan::SendableRecordBatchStream;
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use observability_deps::tracing::*;
use predicate::Predicate;
use schema::selection::Selection;
use schema::Schema;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, mem, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table '{}' not found in chunk", table_name))]
    NamedTableNotFoundInChunk { table_name: String },

    #[snafu(display("Failed to read parquet: {}", source))]
    ReadParquet { source: crate::storage::Error },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(
        display("Cannot decode parquet metadata from {:?}: {}", path, source),
        visibility(pub)
    )]
    MetadataDecodeFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },

    #[snafu(
        display("Cannot read schema from {:?}: {}", path, source),
        visibility(pub)
    )]
    SchemaReadFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },

    #[snafu(
        display("Cannot read statistics from {:?}: {}", path, source),
        visibility(pub)
    )]
    StatisticsReadFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ChunkMetrics {
    // Placeholder
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metric registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {}
    }

    pub fn new(_metrics: &metric::Registry) -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    /// Meta data of the table
    pub(crate) table_summary: Arc<TableSummary>,

    /// Schema that goes with this table's parquet file
    pub(crate) schema: Arc<Schema>,

    /// min/max time range of this table's parquet file, extracted from Parquet File catalog info
    pub(crate) timestamp_min_max: TimestampMinMax,

    /// Persists the parquet file within a database's relative path
    pub(crate) iox_object_store: Arc<IoxObjectStore>,

    /// Path in the database's object store.
    pub(crate) path: ParquetFilePath,

    /// Size of the data, in object store
    pub(crate) file_size_bytes: usize,

    /// Number of rows
    pub(crate) rows: usize,

    #[allow(dead_code)]
    pub(crate) metrics: ChunkMetrics,
}

impl ParquetChunk {
    /// Creates new chunk from given catalog metadata.
    pub fn new(
        parquet_file: &ParquetFile,
        table_summary: Arc<TableSummary>,
        schema: Arc<Schema>,
        iox_object_store: Arc<IoxObjectStore>,
        metrics: ChunkMetrics,
    ) -> Self {
        let timestamp_min_max =
            TimestampMinMax::new(parquet_file.min_time.get(), parquet_file.max_time.get());

        let path = ParquetFilePath::new_new_gen(
            parquet_file.namespace_id,
            parquet_file.table_id,
            parquet_file.sequencer_id,
            parquet_file.partition_id,
            parquet_file.object_store_id,
        );

        let file_size_bytes = parquet_file.file_size_bytes as usize;
        let rows = parquet_file.row_count as usize;

        Self {
            table_summary,
            schema,
            timestamp_min_max,
            iox_object_store,
            path,
            file_size_bytes,
            rows,
            metrics,
        }
    }

    /// Return object store path for this chunk
    pub fn path(&self) -> &ParquetFilePath {
        &self.path
    }

    /// Returns the summary statistics for this chunk
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        &self.table_summary
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.table_summary.size()
            + mem::size_of_val(&self.schema.as_ref())
            + mem::size_of_val(&self.path)
    }

    /// Infallably return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    // Return true if this chunk contains values within the time
    // range, or if the range is `None`.
    pub fn has_timerange(&self, timestamp_range: Option<&TimestampRange>) -> bool {
        match timestamp_range {
            Some(timestamp_range) => self.timestamp_min_max.overlaps(*timestamp_range),
            // If there no range specified,
            None => true,
        }
    }

    // Return the columns names that belong to the given column selection
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
        let fields = self.schema.inner().fields().iter();

        Some(match selection {
            Selection::Some(cols) => fields
                .filter_map(|x| {
                    if cols.contains(&x.name().as_str()) {
                        Some(x.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
            Selection::All => fields.map(|x| x.name().clone()).collect(),
        })
    }

    /// Return stream of data read from parquet file
    pub fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(path=?self.path, "fetching parquet data for filtered read");
        Storage::read_filter(
            predicate,
            selection,
            Arc::clone(&self.schema.as_arrow()),
            self.path.clone(),
            Arc::clone(&self.iox_object_store),
        )
        .context(ReadParquetSnafu)
    }

    /// The total number of rows in all row groups in this chunk.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Size of the parquet file in object store
    pub fn file_size_bytes(&self) -> usize {
        self.file_size_bytes
    }
}

// Parquet file with decoded metadata.
#[derive(Debug)]
pub struct DecodedParquetFile {
    pub parquet_file: ParquetFile,
    pub parquet_metadata: Arc<IoxParquetMetaData>,
    pub decoded_metadata: DecodedIoxParquetMetaData,
    pub iox_metadata: IoxMetadata,
}

impl DecodedParquetFile {
    pub fn new(parquet_file_with_metadata: ParquetFileWithMetadata) -> Self {
        let (parquet_file, parquet_metadata) = parquet_file_with_metadata.split_off_metadata();
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(parquet_metadata));
        let decoded_metadata = parquet_metadata.decode().expect("parquet metadata broken");
        let iox_metadata = decoded_metadata
            .read_iox_metadata_new()
            .expect("cannot read IOx metadata from parquet MD");

        Self {
            parquet_file,
            parquet_metadata,
            decoded_metadata,
            iox_metadata,
        }
    }
}
