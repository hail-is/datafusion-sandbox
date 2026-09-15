use crate::{
    locus::StoredOrdering,
    sink::{self, SinkTarget},
};

use datafusion::{
    arrow::{
        array::{Array, UInt64Array},
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
    physical_plan::ExecutionPlan,
    prelude::DataFrame,
};
use datafusion_datasource::{file_groups::FileGroup, file_sink_config::FileOutputMode};
use std::{collections::HashMap, fmt, sync::Arc};
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex_datafusion::{VortexFormat, VortexFormatFactory};

#[derive(Debug)]
pub struct InputFormat(InputRepr);

#[derive(Debug)]
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

    /// Writes all rows in `df` to `path`, returning the number of rows written. `ordering` is the
    /// order the rows must arrive at the file sink in, which the sink requires of its input; `None`
    /// places no requirement.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row count.
    pub async fn write(
        &self,
        df: DataFrame,
        path: &str,
        ordering: Option<&StoredOrdering>,
    ) -> Result<u64> {
        let batches = self.sink_frame(df, path, ordering)?.collect().await?;
        decode_row_count(&batches)
    }

    /// The frame that writes all rows in `df` to `path` when executed: `df` under this format's
    /// file sink, which requires `ordering` of its input.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built.
    pub fn sink_frame(
        &self,
        df: DataFrame,
        path: &str,
        ordering: Option<&StoredOrdering>,
    ) -> Result<DataFrame> {
        let target = Arc::new(FileSinkTarget {
            factory: self.output_factory(),
            format_options: self.format_options(),
            path: path.to_string(),
        });
        sink::run_into(df, path, ordering, target)
    }

    fn output_factory(&self) -> Arc<dyn FileFormatFactory> {
        match self.0 {
            OutputRepr::Parquet { .. } => Arc::new(ParquetFormatFactory::new()),
            OutputRepr::Vortex { .. } => Arc::new(VortexFormatFactory::new()),
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

/// A format's file sink at a path, built the way `COPY TO` builds it except that the caller
/// supplies the ordering requirement instead of the planner deriving one from the input.
#[derive(Debug)]
struct FileSinkTarget {
    factory: Arc<dyn FileFormatFactory>,
    format_options: HashMap<String, String>,
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
        let format = self.factory.create(state, &self.format_options)?;
        let file_extension = format.compression_type().map_or_else(
            || format.get_ext(),
            |compression| {
                format
                    .get_ext_with_compression(&compression)
                    .unwrap_or_else(|_| format.get_ext())
            },
        );
        let table_path = ListingTableUrl::parse(&self.path)?;
        let config = FileSinkConfig {
            original_url: self.path.clone(),
            object_store_url: table_path.object_store(),
            table_paths: vec![table_path],
            file_group: FileGroup::default(),
            output_schema: input.schema(),
            table_partition_cols: Vec::new(),
            insert_op: InsertOp::Append,
            keep_partition_by_columns: state.config_options().execution.keep_partition_by_columns,
            file_extension,
            file_output_mode: FileOutputMode::Automatic,
        };
        format
            .create_writer_physical_plan(input, state, config, ordering)
            .await
    }
}

fn decode_row_count(batches: &[RecordBatch]) -> Result<u64> {
    match batches {
        [batch] if batch.num_columns() == 1 && batch.num_rows() == 1 => batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .filter(|counts| !counts.is_null(0))
            .map(|counts| counts.value(0)),
        _ => None,
    }
    .ok_or_else(|| {
        DataFusionError::Internal(format!(
            "expected one batch with a single non-null count: UInt64 row from copy-to, got {batches:?}"
        ))
    })
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
