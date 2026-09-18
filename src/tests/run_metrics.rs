//! The run record and run metrics tables, filled from a generated-table plan that touches no
//! store.

use crate::{
    generated::make_range_table,
    pipeline::{self, PipelineOptions},
    run_metrics::{self, RunRecord},
    sink::{self, CollectingSink, DataSinkTarget},
    tests::support::{string_values, timestamp_values, u64_values},
};

use datafusion::{
    arrow::{array::Array, record_batch::RecordBatch, util::display::array_value_to_string},
    error::Result,
    prelude::{DataFrame, SessionContext, col, lit},
};

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[test]
fn the_run_record_batch_carries_the_facts_it_is_given() {
    let record = RunRecord {
        run_id: "run-1".to_string(),
        started_at: SystemTime::UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
        formulation: "grouped-merge".to_string(),
        groups: Some(3),
        split_points: None,
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        output_format: "parquet".to_string(),
        compression: Some("zstd(3)".to_string()),
        threads: 4,
        samples: 50,
        output_path: "gs://bucket/combined.parquet".to_string(),
        rows_written: 123_456,
        run_ns: 2_000_000_000,
        execute_ns: 1_500_000_000,
    };

    let batch = run_metrics::run_record_batch(&record).unwrap();

    assert_eq!(batch.schema(), run_metrics::run_record_schema());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        cells(&batch, 0),
        [
            "run-1",
            "2023-11-14T22:13:20.123456789Z",
            "grouped-merge",
            "3",
            "",
            "gs://bucket/refs",
            "vortex",
            "parquet",
            "zstd(3)",
            "4",
            "50",
            "gs://bucket/combined.parquet",
            "123456",
            "2000000000",
            "1500000000",
        ]
    );
    assert!(batch.column_by_name("split_points").unwrap().is_null(0));
    assert!(!batch.column_by_name("compression").unwrap().is_null(0));
}

/// One row's place in the plan tree: operator, node, parent, depth, partition.
type TreeRow = (String, u64, Option<u64>, u64, Option<u64>);

/// Row `row` of `batch` rendered cell by cell, a null rendering as the empty string.
fn cells(batch: &RecordBatch, row: usize) -> Vec<String> {
    batch
        .columns()
        .iter()
        .map(|column| array_value_to_string(column, row).unwrap())
        .collect()
}

/// A union of two generated tables under a collecting sink yields one row per operator per
/// partition: the sink, which reports no metrics, is one row of nulls; the coalesce above the
/// union runs one partition; the union runs one partition per table; each table's stream reports
/// no metrics. Node index, parent, and depth reconstruct the tree in pre-order, and the baseline
/// columns hold what each partition saw.
#[test]
fn the_run_metrics_batch_holds_one_row_per_operator_per_partition() {
    let executed = execute(|ctx| {
        let left = make_range_table(ctx, 1000, 128)?;
        let right = make_range_table(ctx, 500, 128)?;
        left.union(right)
    });

    let metrics = run_metrics::run_metrics_batch("run-1", &executed.plan).unwrap();

    assert_eq!(metrics.batch.schema(), run_metrics::run_metrics_schema());
    assert_eq!(metrics.unrecorded, Vec::<String>::new());
    let batch = &metrics.batch;
    let tree: Vec<TreeRow> = (0..batch.num_rows())
        .map(|row| {
            (
                string_values(batch, "operator")[row].clone(),
                u64_values(batch, "node")[row].unwrap(),
                u64_values(batch, "parent")[row],
                u64_values(batch, "depth")[row].unwrap(),
                u64_values(batch, "partition")[row],
            )
        })
        .collect();
    assert_eq!(
        tree,
        [
            ("DataSinkExec".to_string(), 0, None, 0, None),
            ("CoalescePartitionsExec".to_string(), 1, Some(0), 1, Some(0)),
            ("UnionExec".to_string(), 2, Some(1), 2, Some(0)),
            ("UnionExec".to_string(), 2, Some(1), 2, Some(1)),
            ("StreamingTableExec".to_string(), 3, Some(2), 3, None),
            ("StreamingTableExec".to_string(), 4, Some(2), 3, None),
        ],
        "{batch:?}"
    );
    assert_eq!(
        string_values(batch, "run_id"),
        vec!["run-1"; 6],
        "{batch:?}"
    );
    assert!(
        string_values(batch, "display")[0].starts_with("DataSinkExec: sink=CollectingSink"),
        "{batch:?}"
    );
    assert_eq!(
        u64_values(batch, "output_rows"),
        [None, Some(1500), Some(1000), Some(500), None, None],
        "{batch:?}"
    );
    assert_eq!(
        u64_values(batch, "output_batches"),
        [None, Some(12), Some(8), Some(4), None, None],
        "{batch:?}"
    );
    for column in ["output_bytes", "elapsed_compute"] {
        let values = u64_values(batch, column);
        assert!(
            values[1..4]
                .iter()
                .all(|value| value.is_some_and(|v| v > 0)),
            "{column}: {values:?}"
        );
        assert!(
            values[0].is_none() && values[4].is_none() && values[5].is_none(),
            "{column}: {values:?}"
        );
    }
    let start = timestamp_values(batch, "start_timestamp");
    let end = timestamp_values(batch, "end_timestamp");
    for row in 1..4 {
        assert!(
            start[row].is_some_and(|start| end[row].is_some_and(|end| start <= end)),
            "row {row}: {start:?} {end:?}"
        );
    }
    for row in [0, 4, 5] {
        assert!(start[row].is_none() && end[row].is_none(), "row {row}");
    }
}

/// A metric the schema has no column for is dropped and its name reported once, and the operator
/// that reported it still gets its baseline row. An aggregate records metrics beyond the baseline,
/// its spill counters among them.
#[test]
fn an_unknown_metric_is_reported_and_not_recorded() {
    let executed = execute(|ctx| {
        make_range_table(ctx, 1000, 128)?
            .aggregate(vec![(col("idx") % lit(10)).alias("bucket")], vec![])
    });

    let metrics = run_metrics::run_metrics_batch("run-2", &executed.plan).unwrap();

    assert!(
        metrics.unrecorded.contains(&"spill_count".to_string()),
        "{:?}",
        metrics.unrecorded
    );
    assert!(metrics.unrecorded.is_sorted(), "{:?}", metrics.unrecorded);
    let mut deduplicated = metrics.unrecorded.clone();
    deduplicated.dedup();
    assert_eq!(deduplicated, metrics.unrecorded);
    for baseline in [
        "output_rows",
        "output_batches",
        "output_bytes",
        "elapsed_compute",
        "start_timestamp",
        "end_timestamp",
    ] {
        assert!(
            !metrics.unrecorded.contains(&baseline.to_string()),
            "{:?}",
            metrics.unrecorded
        );
    }
    assert_eq!(
        metrics
            .batch
            .schema()
            .fields()
            .iter()
            .filter(|f| f.name() == "spill_count")
            .count(),
        0
    );
    // The final aggregate runs one partition per target partition; its rows sum to the buckets.
    let batch = &metrics.batch;
    let operators = string_values(batch, "operator");
    let nodes = u64_values(batch, "node");
    let final_aggregate = operators
        .iter()
        .position(|operator| operator == "AggregateExec")
        .map_or_else(|| panic!("{batch:?}"), |row| nodes[row]);
    let output_rows: u64 = u64_values(batch, "output_rows")
        .iter()
        .zip(&nodes)
        .filter(|(_, node)| **node == final_aggregate)
        .map(|(rows, _)| rows.unwrap_or_default())
        .sum();
    assert_eq!(output_rows, 10, "{batch:?}");
}

/// Runs the frame `build` returns into a collecting sink on the pipeline runner and hands back
/// the executed plan.
fn execute(
    build: impl FnOnce(&SessionContext) -> Result<DataFrame> + Send + 'static,
) -> sink::ExecutedSink {
    pipeline::run(
        move |ctx| async move {
            let frame = build(&ctx)?;
            let collecting = Arc::new(CollectingSink::new(Arc::clone(frame.schema().inner())));
            let target = Arc::new(DataSinkTarget::new(collecting));
            sink::execute_and_retain(sink::run_into(frame, "collect", None, target)?).await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}
