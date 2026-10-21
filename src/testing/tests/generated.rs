//! Generated in-memory tables: integer ranges, an endless repetition, and a grouped count and a
//! join over ranges.

use crate::{
    pipeline::{self, PipelineOptions},
    testing::generated::{
        make_range_table, make_table_group_by_aggregate_sorted, make_table_range_join,
        make_unbounded_table,
    },
};

use datafusion::{
    arrow::{
        array::{Array, Int32Array, Int64Array},
        record_batch::RecordBatch,
    },
    error::Result,
    physical_plan::ExecutionPlanProperties,
    prelude::{DataFrame, SessionContext, col},
};

/// The batches of the frame `build` makes, executed in the runner's session.
fn collected(
    build: impl FnOnce(&SessionContext) -> Result<DataFrame> + Send + 'static,
) -> Vec<RecordBatch> {
    pipeline::run(
        move |ctx| async move { build(&ctx)?.collect().await },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

fn int32_values(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let column = batch.column_by_name(name).unwrap();
    assert_eq!(column.null_count(), 0, "{name}");
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn a_range_table_holds_1_through_n_rows_in_batches_of_the_batch_size() {
    let batches = collected(|ctx| make_range_table(ctx, 10, 4));

    let values: Vec<Vec<i32>> = batches
        .iter()
        .map(|batch| int32_values(batch, "idx"))
        .collect();
    assert_eq!(values, [vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10]]);
}

#[test]
fn a_range_table_of_no_rows_is_empty() {
    let batches = collected(|ctx| make_range_table(ctx, 0, 4));

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[test]
fn a_range_table_declares_its_ascending_order() {
    let plan = pipeline::run(
        |ctx| async move { make_range_table(&ctx, 10, 4)?.create_physical_plan().await },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    let ordering = plan.output_ordering().expect("a declared ordering");
    assert_eq!(ordering.to_string(), "idx@0 ASC");
}

#[test]
fn a_range_table_refuses_unsupported_sizes() {
    let ctx = SessionContext::new();
    let too_large = u32::try_from(i32::MAX).unwrap() + 1;

    for (n_rows, batch_size, reason) in [
        (10, 0, "batch size must be at least 1"),
        (10, too_large, "batch size"),
        (too_large, 4, "row count"),
        (too_large - 1, 4, "headroom"),
    ] {
        let error = make_range_table(&ctx, n_rows, batch_size).unwrap_err();

        assert!(
            error.to_string().contains(reason),
            "{n_rows} rows of batch size {batch_size}: {error}"
        );
    }
}

#[test]
fn an_unbounded_table_repeats_1_through_its_batch_size_without_end() {
    let batches = collected(|ctx| make_unbounded_table(ctx, 3)?.limit(0, Some(8)));
    let plan = pipeline::run(
        |ctx| async move { make_unbounded_table(&ctx, 3)?.create_physical_plan().await },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    let values: Vec<i32> = batches
        .iter()
        .flat_map(|batch| int32_values(batch, "idx"))
        .collect();
    assert_eq!(values, [1, 2, 3, 1, 2, 3, 1, 2]);
    assert!(plan.boundedness().is_unbounded());
}

#[test]
fn an_unbounded_table_refuses_unsupported_batch_sizes() {
    let ctx = SessionContext::new();
    let too_large = u32::try_from(i32::MAX).unwrap() + 1;

    for batch_size in [0, too_large] {
        let error = make_unbounded_table(&ctx, batch_size).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("batch size must be between 1 and"),
            "{batch_size}: {error}"
        );
    }
}

#[test]
fn the_grouped_count_counts_each_thousand_of_the_range() {
    let batches = collected(|ctx| {
        make_table_group_by_aggregate_sorted(ctx, 256, 2_500)?.sort_by(vec![col("group")])
    });

    let counts: Vec<(i32, i64)> = batches
        .iter()
        .flat_map(|batch| {
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec();
            int32_values(batch, "group").into_iter().zip(counts)
        })
        .collect();
    assert_eq!(counts, [(0, 999), (1, 1_000), (2, 501)]);
}

#[test]
fn the_range_join_matches_each_integer_both_ranges_hold() {
    let batches = collected(|ctx| make_table_range_join(ctx, 5, 3, 2)?.sort_by(vec![col("idx")]));

    let pairs: Vec<(i32, i32)> = batches
        .iter()
        .flat_map(|batch| {
            int32_values(batch, "idx")
                .into_iter()
                .zip(int32_values(batch, "idx2"))
        })
        .collect();
    assert_eq!(pairs, [(1, 1), (2, 2), (3, 3)]);
}
