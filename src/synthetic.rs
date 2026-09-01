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

impl IntRangeStream {
    fn new(schema: SchemaRef, start: i32, end: i32, batch_size: u32) -> Result<Self> {
        let batch_size = i32::try_from(batch_size).map_err(|_| {
            DataFusionError::Plan(format!(
                "range table batch size {batch_size} exceeds the maximum supported value {}",
                i32::MAX
            ))
        })?;
        if batch_size == 0 {
            return Err(DataFusionError::Plan(
                "range table batch size must be at least 1".to_string(),
            ));
        }
        if start <= end && end.checked_add(batch_size).is_none() {
            return Err(DataFusionError::Plan(format!(
                "range end {end} leaves insufficient integer headroom for batch size {batch_size}"
            )));
        }

        Ok(Self {
            schema,
            start,
            end,
            batch_size,
        })
    }
}

impl PartitionStream for IntRangeStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[allow(clippy::arithmetic_side_effects)]
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
                // `new` proves `end + batch_size` fits, and `next <= end` here.
                let last = (next + batch_size - 1).min(end);
                let array = Int32Array::from_iter_values(next..=last);
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
                    .map_err(DataFusionError::from);
                // `last <= end`, and `new` proves at least one integer of headroom.
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

    let partition: Arc<dyn PartitionStream> = Arc::new(IntRangeStream::new(
        Arc::clone(&schema),
        start,
        end,
        batch_size,
    )?);

    Ok(
        StreamingTable::try_new(schema, vec![partition])?.with_sort_order(vec![SortExpr::new(
            col("idx"),
            true,
            true,
        )]),
    )
}

pub fn make_range_table(ctx: &SessionContext, n_rows: u32, batch_size: u32) -> Result<DataFrame> {
    let end = i32::try_from(n_rows).map_err(|_| {
        DataFusionError::Plan(format!(
            "range table row count {n_rows} exceeds the maximum supported value {}",
            i32::MAX
        ))
    })?;
    let range_provider = Arc::new(make_range_table_source(1, end, batch_size)?);
    ctx.read_table(range_provider)
}

// DataFusion overloads division to build an expression; it performs no arithmetic here.
#[allow(clippy::arithmetic_side_effects)]
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
