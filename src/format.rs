use crate::{
    locus::StoredOrdering,
    ordered_frame::{OrderedFrame, OutputLayout},
    sink::{self, PartitionedSinkExec, SinkTarget},
};

use datafusion::{
    arrow::{
        array::{Array, UInt64Array},
        datatypes::SchemaRef,
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::file_options::parquet_writer,
    datasource::{
        file_format::{
            FileFormat, FileFormatFactory,
            parquet::{ParquetFormat, ParquetFormatFactory},
        },
        listing::ListingTableUrl,
        physical_plan::FileSinkConfig,
    },
    error::{DataFusionError, Result},
    logical_expr::dml::InsertOp,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
    prelude::DataFrame,
};
use datafusion_datasource::{file_groups::FileGroup, file_sink_config::FileOutputMode};
use std::{collections::HashMap, fmt, sync::Arc};
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex_datafusion::{VortexFormat, VortexFormatFactory};

#[derive(Clone, Debug)]
pub struct InputFormat(InputRepr);

#[derive(Clone, Debug)]
enum InputRepr {
    Parquet,
    Vortex,
}

impl InputFormat {
    pub const PARQUET: Self = Self(InputRepr::Parquet);
    pub const VORTEX: Self = Self(InputRepr::Vortex);

    /// The `DataFusion` reader for files in this format.
    #[must_use]
    pub fn read_format(&self) -> Arc<dyn FileFormat> {
        match self.0 {
            InputRepr::Parquet => Arc::new(ParquetFormat::default()),
            InputRepr::Vortex => Arc::new(VortexFormat::new(VortexSession::default())),
        }
    }
}

#[derive(Debug)]
pub struct OutputFormat(OutputRepr);

#[derive(Debug)]
enum OutputRepr {
    Parquet { compression: Option<String> },
    Vortex { compact: Option<bool> },
}

impl OutputFormat {
    pub const PARQUET: Self = Self(OutputRepr::Parquet { compression: None });
    pub const VORTEX: Self = Self(OutputRepr::Vortex { compact: None });

    /// Applies `compression`, failing when this format does not accept it.
    ///
    /// # Errors
    ///
    /// Returns an error if `compression` is invalid for this output format.
    pub fn with_compression(mut self, compression: &str) -> Result<Self> {
        match &mut self.0 {
            OutputRepr::Parquet {
                compression: parquet_compression,
            } => {
                // DataFusion 55's parser assumes anything after `(` ends with `)` and removes the
                // final byte. An input such as `gzip(` leaves an empty suffix, so the parser
                // panics instead of returning a configuration error.
                if !has_parquet_compression_syntax(compression)
                    || parquet_writer::parse_compression_string(compression).is_err()
                {
                    return Err(unrecognized_compression(compression, "parquet"));
                }
                *parquet_compression = Some(compression.to_string());
            }
            OutputRepr::Vortex { compact } => {
                *compact = Some(match compression {
                    "standard" => false,
                    "compact" => true,
                    _ => return Err(unrecognized_compression(compression, "vortex")),
                });
            }
        }
        Ok(self)
    }

    #[must_use]
    pub const fn extension(&self) -> &'static str {
        match self.0 {
            OutputRepr::Parquet { .. } => "parquet",
            OutputRepr::Vortex { .. } => "vortex",
        }
    }

    /// Writes an ordered frame to `path`, returning the number of rows written.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row counts.
    pub async fn write(&self, ordered: OrderedFrame, path: &str) -> Result<u64> {
        let batches = self.sink_frame(ordered, path)?.collect().await?;
        decode_row_count(&batches)
    }

    /// Writes one file of unordered rows to `path`, returning the number of rows written.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row counts.
    pub async fn write_unordered(&self, frame: DataFrame, path: &str) -> Result<u64> {
        let batches = self
            .sink_frame_with(frame, path, None, OutputLayout::SingleFile)?
            .collect()
            .await?;
        decode_row_count(&batches)
    }

    /// The frame that writes `ordered` to `path` when executed.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built.
    pub fn sink_frame(&self, ordered: OrderedFrame, path: &str) -> Result<DataFrame> {
        let OrderedFrame {
            frame,
            ordering,
            layout,
        } = ordered;
        self.sink_frame_with(frame, path, Some(&ordering), layout)
    }

    fn sink_frame_with(
        &self,
        frame: DataFrame,
        path: &str,
        ordering: Option<&StoredOrdering>,
        layout: OutputLayout,
    ) -> Result<DataFrame> {
        let target: Arc<dyn SinkTarget> = match layout {
            OutputLayout::SingleFile => Arc::new(FileSinkTarget {
                output: self.output_factory(),
                path: path.to_string(),
            }),
            OutputLayout::FilePerPartition => Arc::new(PartitionedFileSinkTarget {
                output: self.output_factory(),
                directory: path.to_string(),
            }),
        };
        sink::run_into(frame, path, ordering, target)
    }

    /// The path of the file holding partition `index` of `count` when this format writes a
    /// directory at `directory` in [`OutputLayout::FilePerPartition`], with this format's
    /// extension. A format with a compression suffix would add it to the extension when it
    /// writes; neither format here has one.
    #[must_use]
    pub fn partition_file_path(&self, directory: &str, index: usize, count: usize) -> String {
        partition_file_path(directory, index, count, self.extension())
    }

    fn output_factory(&self) -> OutputFactory {
        let factory: Arc<dyn FileFormatFactory> = match self.0 {
            OutputRepr::Parquet { .. } => Arc::new(ParquetFormatFactory::new()),
            OutputRepr::Vortex { .. } => Arc::new(VortexFormatFactory::new()),
        };
        OutputFactory {
            factory,
            format_options: self.format_options(),
        }
    }

    fn format_options(&self) -> HashMap<String, String> {
        match &self.0 {
            OutputRepr::Parquet { compression: None } | OutputRepr::Vortex { compact: None } => {
                HashMap::new()
            }
            OutputRepr::Parquet {
                compression: Some(compression),
            } => HashMap::from([("format.compression".to_string(), compression.clone())]),
            OutputRepr::Vortex {
                compact: Some(compact),
            } => HashMap::from([(
                "format.use_compact_encodings".to_string(),
                compact.to_string(),
            )]),
        }
    }
}

/// The factory and options that make a format's writer on a session.
#[derive(Debug)]
struct OutputFactory {
    factory: Arc<dyn FileFormatFactory>,
    format_options: HashMap<String, String>,
}

impl OutputFactory {
    /// The writer on `state`, with this output's options applied over the session's defaults.
    fn create(&self, state: &dyn Session) -> Result<Arc<dyn FileFormat>> {
        self.factory.create(state, &self.format_options)
    }
}

/// A format's file sink at a path, built the way `COPY TO` builds it except that the caller
/// supplies the ordering requirement instead of the planner deriving one from the input.
#[derive(Debug)]
struct FileSinkTarget {
    output: OutputFactory,
    path: String,
}

#[async_trait::async_trait]
impl SinkTarget for FileSinkTarget {
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let format = self.output.create(state)?;
        let config = single_file_sink_config(state, format.as_ref(), &self.path, input.schema())?;
        format
            .create_writer_physical_plan(input, state, config, ordering)
            .await
    }
}

/// A format's file sinks under one partitioned sink: one single-file sink per partition of the
/// input, at the partition's path in `directory`, each built the way [`FileSinkTarget`] builds
/// its one sink.
///
/// The format builds every partition's sink itself, so each gets what the format adds beyond the
/// sink's constructor: Parquet's sorting-column metadata from the ordering, Vortex's compact
/// encodings from the compression option, and the session's table options. See ADR 0015.
#[derive(Debug)]
struct PartitionedFileSinkTarget {
    output: OutputFactory,
    directory: String,
}

#[async_trait::async_trait]
impl SinkTarget for PartitionedFileSinkTarget {
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let format = self.output.create(state)?;
        let count = input.output_partitioning().partition_count();
        let extension = file_extension(format.as_ref());
        let mut partition_sinks = Vec::with_capacity(count);
        for index in 0..count {
            let path = partition_file_path(&self.directory, index, count, &extension);
            let config = single_file_sink_config(state, format.as_ref(), &path, input.schema())?;
            let partition = PartitionedSinkExec::input_partition(Arc::clone(&input), index);
            partition_sinks.push(
                format
                    .create_writer_physical_plan(partition, state, config, ordering.clone())
                    .await?,
            );
        }
        Ok(Arc::new(PartitionedSinkExec::try_new(
            input,
            partition_sinks,
            ordering,
        )?))
    }
}

/// The sink configuration writing one file of `format` at `path`, as `COPY TO` would configure it.
fn single_file_sink_config(
    state: &dyn Session,
    format: &dyn FileFormat,
    path: &str,
    output_schema: SchemaRef,
) -> Result<FileSinkConfig> {
    let table_path = ListingTableUrl::parse(path)?;
    Ok(FileSinkConfig {
        original_url: path.to_string(),
        object_store_url: table_path.object_store(),
        table_paths: vec![table_path],
        file_group: FileGroup::default(),
        output_schema,
        table_partition_cols: Vec::new(),
        insert_op: InsertOp::Append,
        keep_partition_by_columns: state.config_options().execution.keep_partition_by_columns,
        file_extension: file_extension(format),
        file_output_mode: FileOutputMode::Automatic,
    })
}

/// The path of the file holding partition `index` of `count` in `directory`: the index
/// zero-padded to the digit count of `count`, with `extension`.
fn partition_file_path(directory: &str, index: usize, count: usize, extension: &str) -> String {
    let width = count.to_string().len();
    format!(
        "{}/{index:0width$}.{extension}",
        directory.trim_end_matches('/')
    )
}

/// The extension `format` writes, with its compression suffix when it has one.
fn file_extension(format: &dyn FileFormat) -> String {
    format.compression_type().map_or_else(
        || format.get_ext(),
        |compression| {
            format
                .get_ext_with_compression(&compression)
                .unwrap_or_else(|_| format.get_ext())
        },
    )
}

/// The rows written, summed over the count batches a sink plan yields: one from a single-file
/// sink, one per partition from a partitioned sink.
fn decode_row_count(batches: &[RecordBatch]) -> Result<u64> {
    let malformed = || {
        DataFusionError::Internal(format!(
            "expected batches of one non-null count: UInt64 column from the sink, got {batches:?}"
        ))
    };
    if batches.is_empty() {
        return Err(malformed());
    }
    let mut total = 0_u64;
    for batch in batches {
        if batch.num_columns() != 1 {
            return Err(malformed());
        }
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .filter(|counts| counts.null_count() == 0)
            .ok_or_else(malformed)?;
        for count in counts.values() {
            total = total.checked_add(*count).ok_or_else(malformed)?;
        }
    }
    Ok(total)
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.extension())
    }
}

fn has_parquet_compression_syntax(compression: &str) -> bool {
    let compression = compression.to_ascii_lowercase();
    if ["uncompressed", "snappy", "lz4", "lz4_raw"].contains(&compression.as_str()) {
        return true;
    }
    ["gzip", "brotli", "zstd"].into_iter().any(|codec| {
        compression
            .strip_prefix(codec)
            .and_then(|suffix| suffix.strip_prefix('('))
            .and_then(|level| level.strip_suffix(')'))
            .is_some_and(|level| !level.is_empty() && level.chars().all(|c| c.is_ascii_digit()))
    })
}

fn unrecognized_compression(compression: &str, output_format: &str) -> DataFusionError {
    DataFusionError::Configuration(format!(
        "compression '{compression}' is not recognized for output format '{output_format}'"
    ))
}

#[cfg(test)]
mod tests {
    //! These tests need private access to writer-option mapping until it moves behind an output-format configuration surface.

    use super::*;

    #[test]
    fn maps_parquet_compression_modes_to_their_writer_options() {
        for compression in [
            "uncompressed",
            "snappy",
            "gzip(6)",
            "brotli(5)",
            "lz4",
            "zstd(7)",
            "lz4_raw",
        ] {
            assert_eq!(
                OutputFormat::PARQUET
                    .with_compression(compression)
                    .unwrap()
                    .format_options(),
                std::collections::HashMap::from([(
                    "format.compression".to_string(),
                    compression.to_string(),
                )]),
            );
        }
    }

    #[test]
    fn maps_vortex_compression_modes_to_compact_encodings() {
        for (compression, use_compact_encodings) in [("standard", "false"), ("compact", "true")] {
            assert_eq!(
                OutputFormat::VORTEX
                    .with_compression(compression)
                    .unwrap()
                    .format_options(),
                std::collections::HashMap::from([(
                    "format.use_compact_encodings".to_string(),
                    use_compact_encodings.to_string(),
                )]),
            );
        }
    }
}
