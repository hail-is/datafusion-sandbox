use datafusion::{
    arrow::{
        array::{Int32Array, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::streaming::StreamingTable,
    common::DataFusionError,
    datasource::{
        file_format::{FileFormat, FileFormatFactory, format_as_file_type},
        listing::{ListingOptions, ListingTable, ListingTableConfig},
    },
    error::Result,
    execution::{
        SendableRecordBatchStream, TaskContext,
        context::{DataFilePaths, SessionConfig},
    },
    functions_aggregate::count::count_all,
    logical_expr::{SortExpr, col, logical_plan::LogicalPlanBuilder},
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
    prelude::*,
};
use std::{collections::HashMap, sync::Arc};
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex_datafusion::VortexFormat;

pub mod combine_alleles;
pub mod combine_refs;
pub mod combine_refs_one_scan;
pub mod cpu_runtime;
pub mod pipeline;

/// The 50 samples of the `1kg_chr22` benchmark dataset.
pub const SAMPLES: &[&str] = &[
    "HG00308", "HG00592", "HG02230", "NA18534", "NA20760", "NA18530", "HG03805", "HG02223",
    "HG00637", "NA12249", "HG02224", "NA21099", "NA11830", "HG01378", "HG00187", "HG01356",
    "HG02188", "NA20769", "HG00190", "NA18618", "NA18507", "HG03363", "NA21123", "HG03088",
    "NA21122", "HG00373", "HG01058", "HG00524", "NA18969", "HG03833", "HG04158", "HG03578",
    "HG00339", "HG00313", "NA20317", "HG00553", "HG01357", "NA19747", "NA18609", "HG01377",
    "NA19456", "HG00590", "HG01383", "HG00320", "HG04001", "NA20796", "HG00323", "HG01384",
    "NA18613", "NA20802",
];

/// The session config the combiners' plan shape depends on. Forcing one partition
/// per input scan is what leaves one partition per sample going into the
/// `SortPreservingMergeExec`; run a combiner under a different config and you get
/// a different plan. Shared so that what the CLI runs and what the plan shape
/// tests assert on cannot drift apart.
pub fn combiner_session_config() -> SessionConfig {
    SessionConfig::new().with_target_partitions(1)
}

#[derive(Debug)]
struct IntRangeStream {
    schema: SchemaRef,
    start: i32,
    end: i32,
    batch_size: i32,
}

impl PartitionStream for IntRangeStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let end = self.end;
        let batch_size = self.batch_size;

        // `unfold` produces one item per poll — that's the laziness.
        let stream = futures::stream::unfold(self.start, move |next| {
            let schema = Arc::clone(&schema);
            async move {
                if next > end {
                    return None;
                }
                let last = (next + batch_size - 1).min(end);
                let array = Int32Array::from_iter_values(next..=last);
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
                    .map_err(DataFusionError::from);
                Some((batch, last + 1))
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub fn make_range_table_source(start: i32, end: i32, batch_size: u32) -> Result<StreamingTable> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)]));

    let partition: Arc<dyn PartitionStream> = Arc::new(IntRangeStream {
        schema: Arc::clone(&schema),
        start,
        end,
        batch_size: batch_size as i32,
    });

    Ok(
        StreamingTable::try_new(schema, vec![partition])?.with_sort_order(vec![SortExpr::new(
            col("idx"),
            true,
            true,
        )]),
    )
}

pub fn make_range_table(ctx: &SessionContext, n_rows: u32, batch_size: u32) -> Result<DataFrame> {
    let range_provider = Arc::new(make_range_table_source(1, n_rows as i32, batch_size)?);
    ctx.read_table(range_provider)
}

pub fn make_table_group_by_aggregate_sorted(
    ctx: &SessionContext,
    batch_size: u32,
    n_rows: u32,
) -> Result<DataFrame> {
    let df = make_range_table(ctx, n_rows, batch_size)?;
    df.aggregate(
        vec![(col("idx") / lit(1000)).alias("group")],
        vec![count_all()],
    )
}

pub fn make_table_range_join(
    ctx: &SessionContext,
    m: u32,
    n: u32,
    batch_size: u32,
) -> Result<DataFrame> {
    let left = make_range_table(ctx, m, batch_size)?;
    let right = make_range_table(ctx, n, batch_size)?.select(vec![col("idx").alias("idx2")])?;
    left.join(right, JoinType::Inner, &["idx"], &["idx2"], None)
}

/// The vortex file format to read through, on a default session.
///
/// Constructing it takes a [`VortexSession`], and every caller here wants the
/// same default one; parquet's format needs no such decision, so it has no
/// counterpart helper.
pub fn vortex_format() -> Arc<dyn FileFormat> {
    Arc::new(VortexFormat::new(VortexSession::default()))
}

/// Reads one or more file collections as a single table.
///
/// Takes the same listing options and optional schema that
/// [`SessionContext::register_listing_table`] does: `listing_options` carries
/// the file format along with the sort order and partition columns declared on
/// the files, and `schema` is inferred from the files when `None`.
///
/// This exists because DataFusion keeps its generic read entry point private,
/// exposing only per-format wrappers. It carries over that entry point's
/// file-statistics cache, so benchmark numbers stay comparable with
/// DataFusion's own readers rather than running uncached. It drops the
/// file-extension check, which DataFusion skips for collection paths anyway,
/// and every path here is a collection.
pub async fn read<P: DataFilePaths>(
    ctx: &SessionContext,
    table_paths: P,
    listing_options: ListingOptions,
    schema: Option<SchemaRef>,
) -> Result<DataFrame> {
    let table_paths = table_paths.to_urls()?;
    if table_paths.is_empty() {
        return Err(DataFusionError::Execution(
            "No table paths were provided".to_string(),
        ));
    }
    let config =
        ListingTableConfig::new_with_multi_paths(table_paths).with_listing_options(listing_options);
    let config = match schema {
        Some(schema) => config.with_schema(schema),
        None => config.infer_schema(&ctx.state()).await?,
    };
    let table = ListingTable::try_new(config)?
        .with_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache());

    ctx.read_table(Arc::new(table))
}

/// Writes all rows in `df` to `path` using `format_factory`.
///
/// DataFusion's parquet, CSV, and JSON DataFrame writers each implement this
/// same operation, but DataFusion exposes no generic DataFrame-level
/// write-with-format entry point. Its public generic seam is
/// [`LogicalPlanBuilder::copy_to`], one layer below `DataFrame`, so this helper
/// uses that supported route.
///
/// This deliberately covers only appending everything to one path, with empty
/// copy options and no partition columns. DataFusion's parquet writer also
/// supports insert options, sort-on-write, and partition columns; those remain
/// known extension points for this helper.
pub async fn write(
    df: DataFrame,
    path: &str,
    format_factory: Arc<dyn FileFormatFactory>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let file_type = format_as_file_type(format_factory);
    let (session_state, plan) = df.into_parts();
    let plan = LogicalPlanBuilder::copy_to(plan, path.into(), file_type, HashMap::new(), vec![])?
        .build()?;
    DataFrame::new(session_state, plan).collect().await
}

/// Decodes the number of rows produced by a DataFusion copy-to write.
///
/// Copy-to returns exactly one batch containing one non-null `UInt64` value in
/// a column named `count`.
pub fn write_count(write_result: &[RecordBatch]) -> Result<u64, DataFusionError> {
    if write_result.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "expected one batch from copy-to, got {}",
            write_result.len()
        )));
    }

    let batch = &write_result[0];
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        return Err(DataFusionError::Internal(format!(
            "expected one column and one row from copy-to, got {} columns and {} rows",
            batch.num_columns(),
            batch.num_rows()
        )));
    }

    let schema = batch.schema();
    let field = schema.field(0);
    if field.name() != "count" || field.data_type() != &DataType::UInt64 || field.is_nullable() {
        return Err(DataFusionError::Internal(format!(
            "expected non-null count: UInt64 from copy-to, got {field:?}"
        )));
    }

    let counts = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("copy-to count column was not a UInt64 array".to_string())
        })?;
    Ok(counts.value(0))
}
