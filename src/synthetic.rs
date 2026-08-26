//! Generated in-memory tables for exercising tests and benchmarks without genomics fixtures.

use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::streaming::StreamingTable,
    common::DataFusionError,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    functions_aggregate::count::count_all,
    logical_expr::SortExpr,
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
    prelude::*,
};
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
