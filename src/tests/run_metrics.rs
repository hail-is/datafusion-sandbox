//! The run record and run metrics tables, filled from a generated-table plan that touches no
//! store.

use crate::{
    generated::make_range_table,
    pipeline::{self, PipelineOptions},
    run_metrics::{self, RunRecord},
    sink::{self, CollectingSink, DataSinkTarget},
    tests::support::{rows_of_operator, string_values, timestamp_values, u64_values},
};

use datafusion::{
    arrow::{
        array::Array,
        datatypes::{DataType, TimeUnit},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    error::Result,
    prelude::{DataFrame, JoinType, SessionContext, col, lit},
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
        peak_rss_bytes: 3_221_225_472,
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
            "3221225472",
        ]
    );
    assert!(batch.column_by_name("split_points").unwrap().is_null(0));
    assert!(!batch.column_by_name("compression").unwrap().is_null(0));
}

/// The run metrics table has a column for every metric the operators present in a current
/// formulation's plan record: the six baseline metrics, the file stream's on every scan, the
/// Parquet scan's, the Vortex scan's counter, the repartition timers, the aggregate's, and the
/// sink's. Pruning metrics flatten to a pruned and a total column, ratios to a numerator and a
/// denominator. Every count and time is an integer; only the two timestamps are timestamps.
#[test]
fn the_run_metrics_schema_has_a_column_for_every_metric_the_present_operators_report() {
    let schema = run_metrics::run_metrics_schema();

    let columns: Vec<(&str, &DataType)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type()))
        .collect();
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let mut expected: Vec<(&str, &DataType)> = vec![
        ("run_id", &DataType::Utf8),
        ("node", &DataType::UInt64),
        ("parent", &DataType::UInt64),
        ("depth", &DataType::UInt64),
        ("operator", &DataType::Utf8),
        ("display", &DataType::Utf8),
        ("partition", &DataType::UInt64),
    ];
    let tree_columns = expected.len();
    expected.extend([
        ("output_rows", &DataType::UInt64),
        ("output_batches", &DataType::UInt64),
        ("output_bytes", &DataType::UInt64),
        ("elapsed_compute", &DataType::UInt64),
        ("start_timestamp", &timestamp),
        ("end_timestamp", &timestamp),
    ]);
    expected.extend(
        [
            // File stream, on every scan.
            "files_opened",
            "files_processed",
            "file_open_errors",
            "file_scan_errors",
            "batches_split",
            "time_elapsed_opening",
            "time_elapsed_scanning_until_data",
            "time_elapsed_scanning_total",
            "time_elapsed_processing",
            // Parquet scan.
            "bytes_scanned",
            "metadata_load_time",
            "pushdown_rows_pruned",
            "pushdown_rows_matched",
            "predicate_evaluation_errors",
            "row_pushdown_eval_time",
            "statistics_eval_time",
            "bloom_filter_eval_time",
            "page_index_eval_time",
            "predicate_cache_inner_records",
            "predicate_cache_records",
            "row_groups_pruned_dynamic_filter",
            "page_index_pages_skipped_by_fully_matched",
            "page_index_load_skipped",
            "files_ranges_pruned_statistics_pruned",
            "files_ranges_pruned_statistics_total",
            "row_groups_pruned_bloom_filter_pruned",
            "row_groups_pruned_bloom_filter_total",
            "limit_pruned_row_groups_pruned",
            "limit_pruned_row_groups_total",
            "row_groups_pruned_statistics_pruned",
            "row_groups_pruned_statistics_total",
            "page_index_pages_pruned_pruned",
            "page_index_pages_pruned_total",
            "page_index_rows_pruned_pruned",
            "page_index_rows_pruned_total",
            "scan_efficiency_ratio_num",
            "scan_efficiency_ratio_den",
            "output_rows_skew_num",
            "output_rows_skew_den",
            // Vortex scan.
            "num_predicate_creation_errors",
            // Repartition.
            "fetch_time",
            "repartition_time",
            "send_time",
            // Aggregate.
            "peak_mem_used",
            "spill_count",
            "spilled_bytes",
            "spilled_rows",
            "skipped_aggregation_rows",
            "time_calculating_group_ids",
            "aggregate_arguments_time",
            "aggregation_time",
            "emitting_time",
            "reduction_factor_num",
            "reduction_factor_den",
            // Sink.
            "rows_written",
            "bytes_written",
        ]
        .into_iter()
        .map(|name| (name, &DataType::UInt64)),
    );
    assert_eq!(columns, expected);
    for field in schema.fields().iter().skip(tree_columns) {
        assert!(field.is_nullable(), "{} is not nullable", field.name());
    }
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

/// An aggregate's own metrics land beside the baseline, none unrecorded: its spill counters as
/// integers and its reduction factor as a numerator and denominator pair, one row per partition.
/// The partial aggregate beneath the final one reduces the 1000 input rows to at most the 10
/// buckets in each of its partitions, so its numerators are its output rows and its denominators
/// sum to the input.
#[test]
fn an_aggregate_records_its_spill_counters_and_reduction_factor() {
    let executed = execute(|ctx| {
        make_range_table(ctx, 1000, 128)?
            .aggregate(vec![(col("idx") % lit(10)).alias("bucket")], vec![])
    });

    let metrics = run_metrics::run_metrics_batch("run-2", &executed.plan).unwrap();

    assert_eq!(metrics.unrecorded, Vec::<String>::new());
    let batch = &metrics.batch;
    let nodes = u64_values(batch, "node");
    let aggregates = rows_of_operator(batch, "AggregateExec");
    let mut aggregate_nodes: Vec<u64> = aggregates.iter().map(|&row| nodes[row].unwrap()).collect();
    aggregate_nodes.dedup();
    assert_eq!(aggregate_nodes.len(), 2, "{batch:?}");
    for &row in &aggregates {
        assert_eq!(u64_values(batch, "spill_count")[row], Some(0), "{batch:?}");
        assert_eq!(
            u64_values(batch, "spilled_bytes")[row],
            Some(0),
            "{batch:?}"
        );
        assert_eq!(u64_values(batch, "spilled_rows")[row], Some(0), "{batch:?}");
        assert!(u64_values(batch, "partition")[row].is_some(), "{batch:?}");
        assert_eq!(u64_values(batch, "files_opened")[row], None, "{batch:?}");
    }
    let partial = aggregate_nodes[1];
    let sum_over_partial = |column: &str| -> u64 {
        u64_values(batch, column)
            .iter()
            .zip(&nodes)
            .filter(|(_, node)| **node == Some(partial))
            .map(|(value, _)| value.unwrap_or_default())
            .sum()
    };
    let partial_partitions =
        u64::try_from(nodes.iter().filter(|node| **node == Some(partial)).count()).unwrap();
    assert_eq!(sum_over_partial("reduction_factor_den"), 1000, "{batch:?}");
    assert_eq!(
        sum_over_partial("reduction_factor_num"),
        sum_over_partial("output_rows"),
        "{batch:?}"
    );
    assert!(
        sum_over_partial("output_rows") <= 10 * partial_partitions,
        "{batch:?}"
    );
    assert_eq!(
        u64_values(batch, "reduction_factor_num")[aggregates[0]],
        None,
        "the final aggregate reports no reduction factor: {batch:?}"
    );
}

/// A metric the schema has no column for is dropped and its name reported once, and the operator
/// that reported it still gets its baseline row. A hash join records its build and probe
/// metrics, which no formulation's plan has.
#[test]
fn an_unknown_metric_is_reported_and_not_recorded() {
    let executed = execute(|ctx| {
        let left = make_range_table(ctx, 100, 128)?;
        let right = make_range_table(ctx, 50, 128)?.select(vec![col("idx").alias("other")])?;
        left.join(right, JoinType::Inner, &["idx"], &["other"], None)
    });

    let metrics = run_metrics::run_metrics_batch("run-3", &executed.plan).unwrap();

    for name in ["build_time", "join_time", "build_input_rows"] {
        assert!(
            metrics.unrecorded.contains(&name.to_string()),
            "{name}: {:?}",
            metrics.unrecorded
        );
    }
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
    let batch = &metrics.batch;
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|f| f.name() != "build_time"),
        "{batch:?}"
    );
    let operators = string_values(batch, "operator");
    let joined: u64 = u64_values(batch, "output_rows")
        .iter()
        .zip(&operators)
        .filter(|(_, operator)| *operator == "HashJoinExec")
        .map(|(rows, _)| rows.unwrap_or_default())
        .sum();
    assert_eq!(joined, 50, "{batch:?}");
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
