//! File writes, their targets, sink frames, and execution.
//!
//! A write target pairs an output path with an output format. An ordered write takes its output
//! layout from the ordered frame and selects the matching file sink. Output-path validation remains
//! the CLI's responsibility.

use crate::{
    format::OutputFormat,
    locus::StoredOrdering,
    ordered_frame::{OrderedFrame, OutputLayout},
    sink::{self, ExecutedSink, PartitionedSinkExec, SinkTarget},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{
        file_format::FileFormat, listing::ListingTableUrl, physical_plan::FileSinkConfig,
    },
    error::Result,
    logical_expr::dml::InsertOp,
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
    prelude::DataFrame,
};
use datafusion_datasource::{file_groups::FileGroup, file_sink_config::FileOutputMode};
use std::sync::Arc;

/// The output path and output format of a write.
#[derive(Debug)]
pub struct WriteTarget {
    pub output_path: String,
    pub output_format: OutputFormat,
}

impl WriteTarget {
    /// The frame that writes `ordered` to this target when executed.
    ///
    /// The ordered frame chooses whether the output path names one file or a directory containing
    /// one file per partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built.
    pub fn sink_frame(&self, ordered: OrderedFrame) -> Result<DataFrame> {
        let OrderedFrame {
            frame,
            ordering,
            layout,
        } = ordered;
        self.sink_frame_with(frame, Some(&ordering), layout)
    }

    /// Writes an ordered frame and returns the sink execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row counts.
    pub async fn write(&self, ordered: OrderedFrame) -> Result<ExecutedSink> {
        sink::execute_and_retain(self.sink_frame(ordered)?).await
    }

    /// Writes one file of unordered rows and returns the sink execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the write plan cannot be built or executed, or if the execution result
    /// does not contain the expected row counts.
    pub async fn write_unordered(&self, frame: DataFrame) -> Result<ExecutedSink> {
        let frame = self.sink_frame_with(frame, None, OutputLayout::SingleFile)?;
        sink::execute_and_retain(frame).await
    }

    fn sink_frame_with(
        &self,
        frame: DataFrame,
        ordering: Option<&StoredOrdering>,
        layout: OutputLayout,
    ) -> Result<DataFrame> {
        let target: Arc<dyn SinkTarget> = match layout {
            OutputLayout::SingleFile => Arc::new(FileSinkTarget {
                output: self.output_format.clone(),
                path: self.output_path.clone(),
            }),
            OutputLayout::FilePerPartition => Arc::new(DirectoryFileSinkTarget {
                output: self.output_format.clone(),
                directory: self.output_path.clone(),
            }),
        };
        sink::run_into(frame, &self.output_path, ordering, target)
    }
}

/// A format's file sink at a path, built the way `COPY TO` builds it except that the caller
/// supplies the ordering requirement instead of the planner deriving one from the input.
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

/// A directory of a format's files under one partitioned sink, with one file per input partition.
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

/// One file of a directory write, using the writer format shared by the directory target.
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

/// The sink configuration writing one file of `format` at `path`, as `COPY TO` configures it.
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
