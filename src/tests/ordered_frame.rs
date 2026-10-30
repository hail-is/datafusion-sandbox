//! The row limit on an ordered frame, which caps its deferred rows and keeps what travels with
//! them; holding an ordered frame in its ordering; and where a write in each output layout holds
//! it.

use crate::{
    locus::LocusRepresentation,
    ordered_frame::{OrderedFrame, OutputLayout},
    ordering_assertion::AssertionScope,
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, StoredOrdering},
};
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    error::Result,
    prelude::{DataFrame, SessionContext, col, lit},
};
use std::sync::Arc;

/// Rows in the frame every test limits.
const ROWS: usize = 5;

/// A schema no limit should touch: a nullable and a non-nullable column of different types.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ])
}

/// An in-memory frame of `ROWS` rows under `schema()`.
fn frame(ctx: &SessionContext) -> DataFrame {
    let batch = RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(Int64Array::from_iter_values(0..5)),
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("c"),
                Some("d"),
                None,
            ])),
        ],
    )
    .unwrap();
    ctx.read_batch(batch).unwrap()
}

/// An ordering other than the plain locus one, so a limit that substitutes a default shows.
fn stored_ordering() -> StoredOrdering {
    RowOrdering::locus_then_sample().expand(LocusRepresentation::ContigPosition)
}

fn ordered(frame: DataFrame, layout: OutputLayout) -> OrderedFrame {
    OrderedFrame {
        frame,
        ordering: stored_ordering(),
        layout,
    }
}

/// Limits the frame `build` makes in the runner's session to `n` rows, then executes it there.
/// The outer result is the limit's, the inner one the execution's row count.
fn limited_row_count(
    build: impl FnOnce(&SessionContext) -> DataFrame + Send + 'static,
    n: usize,
) -> Result<Result<usize>> {
    pipeline::run(
        move |ctx| async move {
            let limited = ordered(build(&ctx), OutputLayout::SingleFile).limit(n)?;
            Ok(limited
                .frame
                .collect()
                .await
                .map(|batches| batches.iter().map(RecordBatch::num_rows).sum()))
        },
        PipelineOptions::single_threaded(),
    )
}

#[test]
fn limit_keeps_the_stored_ordering_and_the_output_layout() {
    for layout in [OutputLayout::SingleFile, OutputLayout::FilePerPartition] {
        let limited = ordered(frame(&SessionContext::new()), layout)
            .limit(2)
            .unwrap();

        assert_eq!(limited.ordering, stored_ordering());
        assert_eq!(limited.layout, layout);
    }
}

#[test]
fn limit_caps_the_rows_at_n() {
    for (n, expected) in [(0, 0), (1, 1), (3, 3), (ROWS, ROWS), (ROWS + 2, ROWS)] {
        let rows = limited_row_count(frame, n).unwrap().unwrap();

        assert_eq!(rows, expected, "n = {n}");
    }
}

#[test]
fn limit_keeps_the_schema() {
    for n in [0, 2] {
        let limited = ordered(frame(&SessionContext::new()), OutputLayout::SingleFile)
            .limit(n)
            .unwrap();

        assert_eq!(limited.frame.schema().as_arrow(), &schema(), "n = {n}");
    }
}

#[test]
fn limit_returns_deferred_work_not_executed_rows() {
    // Dividing by zero fails only when a row is computed, so a limit that ran the frame would
    // fail, and a limit that kept executed rows would execute without failing.
    let failing = |ctx: &SessionContext| {
        frame(ctx)
            .select(vec![(col("n") / lit(0_i64)).alias("n")])
            .unwrap()
    };

    let executed = limited_row_count(failing, 1).expect("the limit executed the frame");

    let error = executed.unwrap_err();
    assert!(error.to_string().contains("Divide by zero"), "{error}");
}

/// Holding a frame places an ordering assertion of its stored ordering, over the scope asked for,
/// at the root of its plan, and hands back that ordering as a sort list.
#[test]
fn hold_asserts_the_stored_ordering_at_the_root_and_returns_it_as_a_sort_list() {
    for (scope, suffix) in [
        (AssertionScope::OnePartition, ""),
        (AssertionScope::EachPartition, ", each partition"),
    ] {
        let keyed = frame(&SessionContext::new())
            .select(vec![
                lit("chr01").alias("contig"),
                col("n").alias("position"),
                col("label").alias("s"),
            ])
            .unwrap();

        let (held, ordering) = ordered(keyed, OutputLayout::SingleFile)
            .hold(scope)
            .unwrap();

        assert_eq!(ordering, stored_ordering().sort_columns());
        let plan = held.logical_plan().display_indent().to_string();
        assert_eq!(
            plan.lines().next(),
            Some(
                format!(
                    "OrderingAssertion: contig ASC NULLS LAST, position ASC NULLS LAST, \
                     s ASC NULLS LAST{suffix}"
                )
                .as_str()
            ),
            "{plan}"
        );
    }
}

#[test]
fn a_single_file_is_written_from_one_partition_held_in_order() {
    assert_eq!(
        OutputLayout::SingleFile.assertion_scope(),
        AssertionScope::OnePartition
    );
}

#[test]
fn a_file_per_partition_write_holds_each_partition_in_order() {
    assert_eq!(
        OutputLayout::FilePerPartition.assertion_scope(),
        AssertionScope::EachPartition
    );
}
