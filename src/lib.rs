use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::streaming::StreamingTable,
    common::DataFusionError,
    datasource::{
        file_format::format_as_file_type,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    functions_aggregate::count::count_all,
    logical_expr::{SortExpr, col, logical_plan::LogicalPlanBuilder},
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
    prelude::*,
};
use std::{collections::HashMap, sync::Arc};
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex_datafusion::{VortexFormat, VortexFormatFactory, VortexTableOptions};

pub mod combine_alleles;
pub mod combine_refs;
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

// TODO: try implementing ReadOptions for this
#[derive(Default, Clone)]
pub struct VortexReadOptions {
    pub file_sort_order: Vec<Vec<SortExpr>>,
    pub table_partition_cols: Vec<(String, DataType)>,
    pub schema: Option<SchemaRef>,
}

impl VortexReadOptions {
    fn to_listing_options(&self) -> ListingOptions {
        let vortex_session = VortexSession::default();
        let file_format = Arc::new(VortexFormat::new(vortex_session));

        ListingOptions::new(file_format)
            .with_file_extension(".vortex")
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_file_sort_order(self.file_sort_order.clone())
    }
}

pub async fn read_vortex(
    ctx: &SessionContext,
    table_path: impl AsRef<str>,
    options: VortexReadOptions,
) -> Result<DataFrame> {
    let table_path = ListingTableUrl::parse(table_path)?;
    let vortex_opts = options.to_listing_options();
    let resolved_schema = match options.schema {
        Some(s) => s,
        None => vortex_opts.infer_schema(&ctx.state(), &table_path).await?,
    };
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(vortex_opts)
        .with_schema(resolved_schema);
    let table = ListingTable::try_new(config)?;
    let df = ctx.read_table(Arc::new(table))?;

    Ok(df)
}

pub async fn write_vortex(
    df: DataFrame,
    path: &str,
    writer_options: Option<VortexTableOptions>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let format = if let Some(vortex_opts) = writer_options {
        Arc::new(VortexFormatFactory::new().with_options(vortex_opts))
    } else {
        Arc::new(VortexFormatFactory::new())
    };

    let file_type = format_as_file_type(format);

    let (session_state, plan) = df.into_parts();

    let plan = LogicalPlanBuilder::copy_to(plan, path.into(), file_type, HashMap::new(), vec![])?
        .build()?;
    DataFrame::new(session_state, plan).collect().await
}
