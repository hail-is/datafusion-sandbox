use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::datasource::file_format::format_as_file_type;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions_aggregate::count::count_all;
use datafusion::logical_expr::logical_plan::LogicalPlanBuilder;
use datafusion::logical_expr::{SortExpr, col};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::*;

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

use vortex_datafusion::{VortexFormat, VortexFormatFactory, VortexTableOptions};

use std::collections::HashMap;
use std::sync::Arc;

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
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let vortex_session = VortexSession::default();
        let file_format = Arc::new(VortexFormat::new(vortex_session));

        ListingOptions::new(file_format)
            .with_file_extension(".vortex")
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_file_sort_order(self.file_sort_order.clone())
            .with_session_config_options(config)
    }
}

pub async fn read_vortex(
    ctx: &SessionContext,
    table_path: impl AsRef<str>,
    options: VortexReadOptions,
) -> Result<DataFrame> {
    let table_path = ListingTableUrl::parse(table_path)?;
    let vortex_opts = options.to_listing_options(ctx.state().config());
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

pub fn read_vortex_with_schema(
    ctx: &SessionContext,
    table_path: impl AsRef<str>,
    options: VortexReadOptions,
) -> Result<DataFrame> {
    let table_path = ListingTableUrl::parse(table_path)?;
    let vortex_opts = options.to_listing_options(ctx.state().config());
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(vortex_opts)
        .with_schema(options.schema.unwrap());
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
