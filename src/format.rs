use crate::{
    locus::StoredOrdering,
    ordered_frame::{OrderedFrame, OutputLayout},
    sink::{self, PartitionedSinkExec, SinkTarget},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
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
    physical_plan::ExecutionPlan,
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

    /// The name of this format: `parquet` or `vortex`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self.0 {
            InputRepr::Parquet => "parquet",
            InputRepr::Vortex => "vortex",
        }
    }

    /// The `DataFusion` reader for files in this format.
    #[must_use]
    pub fn read_format(&self) -> Arc<dyn FileFormat> {
        match self.0 {
            InputRepr::Parquet => Arc::new(ParquetFormat::default()),
            InputRepr::Vortex => Arc::new(VortexFormat::new(VortexSession::default())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OutputFormat(OutputRepr);

#[derive(Clone, Debug)]
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

    /// The name of this format: `parquet` or `vortex`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self.0 {
            OutputRepr::Parquet { .. } => "parquet",
            OutputRepr::Vortex { .. } => "vortex",
        }
    }

    /// The compression mode this format writes with, as a caller would spell it, or `None` when
    /// the format's default applies.
    #[must_use]
    pub fn compression(&self) -> Option<&str> {
        match &self.0 {
            OutputRepr::Parquet { compression } => compression.as_deref(),
            OutputRepr::Vortex { compact } => {
                compact.map(|compact| if compact { "compact" } else { "standard" })
            }
        }
    }

    /// The extension of every file this format writes. It carries no compression suffix because
    /// neither format here has one; a format that does would add it here, and every path named
    /// from it would follow.
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
        Ok(sink::execute_and_retain(self.sink_frame(ordered, path)?)
            .await?
            .rows_written)
    }

    /// Writes one file of unordered rows to `path`, returning the number of rows written.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row counts.
    pub async fn write_unordered(&self, frame: DataFrame, path: &str) -> Result<u64> {
        let frame = self.sink_frame_with(frame, path, None, OutputLayout::SingleFile)?;
        Ok(sink::execute_and_retain(frame).await?.rows_written)
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
                output: self.clone(),
                path: path.to_string(),
            }),
            OutputLayout::FilePerPartition => Arc::new(DirectoryFileSinkTarget {
                output: self.clone(),
                directory: path.to_string(),
            }),
        };
        sink::run_into(frame, path, ordering, target)
    }

    /// The path of the file holding partition `index` of `count` when this format writes a
    /// directory at `directory` in [`OutputLayout::FilePerPartition`]: the index zero-padded to
    /// the digit count of `count`, with this format's extension. The write names its files
    /// through this function, so a caller predicting them names the same files.
    #[must_use]
    pub fn partition_file_path(&self, directory: &str, index: usize, count: usize) -> String {
        let width = count.to_string().len();
        format!(
            "{}/{index:0width$}.{}",
            directory.trim_end_matches('/'),
            self.extension()
        )
    }

    /// The `DataFusion` format that writes files in this format on `state`, with this format's
    /// options applied over the session's defaults.
    fn write_format(&self, state: &dyn Session) -> Result<Arc<dyn FileFormat>> {
        let factory: Arc<dyn FileFormatFactory> = match self.0 {
            OutputRepr::Parquet { .. } => Arc::new(ParquetFormatFactory::new()),
            OutputRepr::Vortex { .. } => Arc::new(VortexFormatFactory::new()),
        };
        factory.create(state, &self.format_options())
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

/// A format's file sink at a path, built the way `COPY TO` builds it except that the caller
/// supplies the ordering requirement instead of the planner deriving one from the input.
///
/// It creates the format itself, because the layout picks a target with no session in hand.
#[derive(Debug)]
struct FileSinkTarget {
    output: OutputFormat,
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
        let format = self.output.write_format(state)?;
        file_sink_plan(state, format.as_ref(), &self.path, input, ordering).await
    }
}

/// A directory of a format's files under one partitioned sink: one file per partition of the
/// input, at the partition's path in `directory`.
///
/// The format builds every partition's sink itself, so each gets what the format adds beyond the
/// sink's constructor: Parquet's sorting-column metadata from the ordering, Vortex's compact
/// encodings from the compression option, and the session's table options. See ADR 0015. The
/// format is created once here and shared by every partition's target.
#[derive(Debug)]
struct DirectoryFileSinkTarget {
    output: OutputFormat,
    directory: String,
}

#[async_trait::async_trait]
impl SinkTarget for DirectoryFileSinkTarget {
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let format = self.output.write_format(state)?;
        let partition = |index: usize, count: usize| -> Arc<dyn SinkTarget> {
            Arc::new(PartitionFileSinkTarget {
                format: Arc::clone(&format),
                path: self
                    .output
                    .partition_file_path(&self.directory, index, count),
            })
        };
        Ok(Arc::new(
            PartitionedSinkExec::plan(state, input, ordering, &partition).await?,
        ))
    }
}

/// One file of a directory write: the same sink [`FileSinkTarget`] plans, over a format the
/// directory's target already created.
#[derive(Debug)]
struct PartitionFileSinkTarget {
    format: Arc<dyn FileFormat>,
    path: String,
}

#[async_trait::async_trait]
impl SinkTarget for PartitionFileSinkTarget {
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        file_sink_plan(state, self.format.as_ref(), &self.path, input, ordering).await
    }
}

/// The plan writing `input` into one file of `format` at `path`, requiring `ordering` of it.
async fn file_sink_plan(
    state: &dyn Session,
    format: &dyn FileFormat,
    path: &str,
    input: Arc<dyn ExecutionPlan>,
    ordering: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = single_file_sink_config(state, format, path, input.schema())?;
    format
        .create_writer_physical_plan(input, state, config, ordering)
        .await
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
        file_extension: format.get_ext(),
        file_output_mode: FileOutputMode::Automatic,
    })
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
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
