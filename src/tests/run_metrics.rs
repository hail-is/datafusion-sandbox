//! The run record and run metrics tables, filled from generated plans and real formulations over
//! in-memory object stores.

use crate::{
    fixture::{self, FixtureFormat, MemoryStore, SAMPLES},
    format::OutputFormat,
    formulation::Formulation,
    generated::make_range_table,
    locus::LocusRepresentation,
    ordered_frame::OutputLayout,
    pipeline::{self, PipelineOptions},
    run_metrics::{self, FormulationRecord, ProbeRecord, RunRecord, WriteRecord},
    sink::{self, CollectingSink, DataSinkTarget},
    stored::dataset::Dataset,
    tests::support::{
        grouped_merge, interval_merge, rows_of_operator, run_record, string_values,
        timestamp_values, u64_values,
    },
    throughput_probe::{Decision, ProbeSettings, ProgressSample, StopReason},
    write::WriteTarget,
};

use datafusion::{
    arrow::{
        array::Array,
        datatypes::{DataType, TimeUnit},
    },
    error::Result,
    prelude::{DataFrame, JoinType, SessionContext, col, lit},
};
use object_store::{ObjectStoreExt, path::Path};

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

/// The run record table's columns, in order, with their types and whether they may be null: the
/// settings a formulation may leave unset, the write a drained probe does not make, and the
/// facts only a probe has are the only nullable ones.
#[test]
fn the_run_record_schema_names_its_columns_in_order() {
    let schema = run_metrics::run_record_schema();

    let columns: Vec<(&str, &DataType, bool)> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable(),
            )
        })
        .collect();
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    assert_eq!(
        columns,
        [
            ("run_id", &DataType::Utf8, false),
            ("started_at", &timestamp, false),
            ("formulation", &DataType::Utf8, false),
            ("groups", &DataType::UInt64, true),
            ("split_points", &DataType::Utf8, true),
            ("dataset_path", &DataType::Utf8, false),
            ("input_format", &DataType::Utf8, false),
            ("output_format", &DataType::Utf8, true),
            ("compression", &DataType::Utf8, true),
            ("threads", &DataType::UInt64, false),
            ("samples", &DataType::UInt64, false),
            ("output_path", &DataType::Utf8, true),
            ("rows_written", &DataType::UInt64, false),
            ("run_ns", &DataType::UInt64, false),
            ("execute_ns", &DataType::UInt64, false),
            ("peak_rss_bytes", &DataType::UInt64, false),
            ("action", &DataType::Utf8, true),
            ("stop_reason", &DataType::Utf8, true),
            ("steady_state_throughput", &DataType::Float64, true),
            ("window_end_ns", &DataType::UInt64, true),
            ("window_rows", &DataType::UInt64, true),
            ("first_partition_end_ns", &DataType::UInt64, true),
            ("poll_period_ns", &DataType::UInt64, true),
            ("max_duration_ns", &DataType::UInt64, true),
        ]
    );
}

/// A record with a distinct value in every field lands each value in the column of the field's
/// name, so no two columns can have swapped accessors unnoticed.
#[test]
fn each_run_record_field_lands_in_the_column_of_its_name() {
    let record = RunRecord {
        run_id: "run-1".to_string(),
        started_at: SystemTime::UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(3),
            split_points: Some("1:5,2:1".to_string()),
        },
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        write: Some(WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("zstd(3)".to_string()),
        }),
        threads: 4,
        samples: 50,
        rows_written: 123_456,
        run_ns: 2_000_000_000,
        execute_ns: 1_500_000_000,
        peak_rss_bytes: 3_221_225_472,
        probe: Some(ProbeRecord {
            settings: ProbeSettings {
                poll_period: Duration::from_millis(100),
                max_duration: Duration::from_secs(300),
            },
            decision: Decision {
                stop_reason: StopReason::Completed,
                steady_state_throughput: Some(2_500.5),
                window_end_ns: 1_400_000_000,
                window_rows: 120_000,
            },
            first_partition_end_ns: Some(1_400_000_001),
        }),
    };

    let batch = run_metrics::run_record_batch(&record).unwrap();

    assert_eq!(batch.schema(), run_metrics::run_record_schema());
    assert_eq!(batch.num_rows(), 1);
    for (column, expected) in [
        ("run_id", "run-1"),
        ("started_at", "2023-11-14T22:13:20.123456789Z"),
        ("formulation", "grouped-merge"),
        ("groups", "3"),
        ("split_points", "1:5,2:1"),
        ("dataset_path", "gs://bucket/refs"),
        ("input_format", "vortex"),
        ("output_format", "parquet"),
        ("compression", "zstd(3)"),
        ("threads", "4"),
        ("samples", "50"),
        ("output_path", "gs://bucket/combined.parquet"),
        ("rows_written", "123456"),
        ("run_ns", "2000000000"),
        ("execute_ns", "1500000000"),
        ("peak_rss_bytes", "3221225472"),
        ("action", "probe"),
        ("stop_reason", "completed"),
        ("steady_state_throughput", "2500.5"),
        ("window_end_ns", "1400000000"),
        ("window_rows", "120000"),
        ("first_partition_end_ns", "1400000001"),
        ("poll_period_ns", "100000000"),
        ("max_duration_ns", "300000000000"),
    ] {
        assert_eq!(string_values(&batch, column), [expected], "{column}");
    }
}

/// A formulation without a group count or split points and a format at its default compression
/// leave those three columns null, and a measured write leaves every probe column null.
#[test]
fn unset_run_record_settings_are_null() {
    let record = RunRecord {
        formulation: FormulationRecord {
            name: "union".to_string(),
            groups: None,
            split_points: None,
        },
        write: Some(WriteRecord {
            compression: None,
            ..run_record("run-1").write.unwrap()
        }),
        ..run_record("run-1")
    };

    let batch = run_metrics::run_record_batch(&record).unwrap();

    for column in [
        "groups",
        "split_points",
        "compression",
        "action",
        "stop_reason",
        "steady_state_throughput",
        "window_end_ns",
        "window_rows",
        "first_partition_end_ns",
        "poll_period_ns",
        "max_duration_ns",
    ] {
        assert!(batch.column_by_name(column).unwrap().is_null(0), "{column}");
    }
    assert_eq!(string_values(&batch, "formulation"), ["union"]);
}

/// A drained probe writes nothing, so its write settings are null; a probe with no estimate and
/// no finished partition leaves those two columns null too.
#[test]
fn a_drained_probe_leaves_its_write_and_missing_facts_null() {
    let record = RunRecord {
        write: None,
        probe: Some(ProbeRecord {
            settings: ProbeSettings::default(),
            decision: Decision {
                stop_reason: StopReason::Capped,
                steady_state_throughput: None,
                window_end_ns: 0,
                window_rows: 0,
            },
            first_partition_end_ns: None,
        }),
        ..run_record("run-1")
    };

    let batch = run_metrics::run_record_batch(&record).unwrap();

    for column in [
        "output_format",
        "compression",
        "output_path",
        "steady_state_throughput",
        "first_partition_end_ns",
    ] {
        assert!(batch.column_by_name(column).unwrap().is_null(0), "{column}");
    }
    assert_eq!(string_values(&batch, "action"), ["probe"]);
    assert_eq!(string_values(&batch, "stop_reason"), ["capped"]);
}

/// The progress samples table holds one row per sample, in the order taken, each with its index
/// in that order.
#[test]
fn progress_samples_fill_one_row_each_in_order() {
    let samples = [(3_000, 0), (100_004_000, 512), (200_001_000, 1_536)]
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows });

    let batch = run_metrics::progress_samples_batch("run-1", &samples).unwrap();

    assert_eq!(batch.schema(), run_metrics::progress_samples_schema());
    let columns: Vec<(&str, &DataType, bool)> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable(),
            )
        })
        .collect();
    assert_eq!(
        columns,
        [
            ("run_id", &DataType::Utf8, false),
            ("sample_index", &DataType::UInt64, false),
            ("elapsed_ns", &DataType::UInt64, false),
            ("rows", &DataType::UInt64, false),
        ]
    );
    assert_eq!(string_values(&batch, "run_id"), ["run-1"; 3]);
    assert_eq!(
        u64_values(&batch, "sample_index"),
        [Some(0), Some(1), Some(2)]
    );
    assert_eq!(
        u64_values(&batch, "elapsed_ns"),
        [Some(3_000), Some(100_004_000), Some(200_001_000)]
    );
    assert_eq!(
        u64_values(&batch, "rows"),
        [Some(0), Some(512), Some(1_536)]
    );
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

/// A file-per-partition write reports its sinks together at the root, while the outer union has
/// one metrics row per locus interval.
#[test]
fn an_interval_merge_records_a_row_per_interval() {
    let wrote = write_formulation(
        FixtureFormat::Vortex,
        OutputFormat::VORTEX,
        interval_merge("1:3,2:2"),
        "metrics-intervals",
    );
    assert_eq!(wrote.executed.rows_written, 32);
    let metrics = run_metrics::run_metrics_batch("run-b", &wrote.executed.plan).unwrap();
    assert_eq!(metrics.unrecorded, Vec::<String>::new());
    let batch = &metrics.batch;

    assert!(
        string_values(batch, "display")[0]
            .starts_with("PartitionedSinkExec: partitions=3, sink=VortexSink"),
        "{batch:?}"
    );
    assert_eq!(u64_values(batch, "partition")[0], None, "{batch:?}");
    assert_eq!(u64_values(batch, "rows_written")[0], Some(32), "{batch:?}");
    assert!(
        u64_values(batch, "bytes_written")[0].is_some_and(|bytes| bytes > 0),
        "{batch:?}"
    );
    assert_eq!(u64_values(batch, "node")[1], Some(1), "{batch:?}");

    let operators = string_values(batch, "operator");
    let nodes = u64_values(batch, "node");
    let outer_union = operators
        .iter()
        .position(|operator| operator == "UnionExec")
        .map_or_else(|| panic!("{batch:?}"), |row| nodes[row]);
    let union_partitions: Vec<Option<u64>> = u64_values(batch, "partition")
        .into_iter()
        .zip(&nodes)
        .filter(|(_, node)| **node == outer_union)
        .map(|(partition, _)| partition)
        .collect();
    assert_eq!(union_partitions, [Some(0), Some(1), Some(2)], "{batch:?}");
}

#[test]
fn every_formulation_in_every_format_reports_no_unrecorded_metric() {
    for fixture_format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for output_format in [OutputFormat::PARQUET, OutputFormat::VORTEX] {
            for (formulation, expected, run_id_suffix) in [
                (Formulation::CombineAllelesUnion, 8, "alleles"),
                (Formulation::CombineRefsUnion, 32, "refs"),
                (grouped_merge(2), 32, "grouped"),
                (interval_merge("1:3,2:2"), 32, "intervals"),
            ] {
                let run_id = format!(
                    "{}-{}-{run_id_suffix}",
                    match fixture_format {
                        FixtureFormat::Parquet => "parquet",
                        FixtureFormat::Vortex => "vortex",
                    },
                    output_format.name(),
                );
                let wrote =
                    write_formulation(fixture_format, output_format.clone(), formulation, &run_id);
                let metrics =
                    run_metrics::run_metrics_batch(&run_id, &wrote.executed.plan).unwrap();

                assert_eq!(wrote.executed.rows_written, expected, "{run_id}");
                assert_eq!(metrics.unrecorded, Vec::<String>::new(), "{run_id}");
            }
        }
    }
}

/// A write's sink row carries the rows and bytes the sink wrote, whichever format it wrote.
#[test]
fn a_write_of_either_format_records_the_rows_and_bytes_its_sink_wrote() {
    for output_format in [OutputFormat::PARQUET, OutputFormat::VORTEX] {
        let run_id = output_format.name();
        let wrote = write_formulation(
            FixtureFormat::Vortex,
            output_format,
            Formulation::CombineRefsUnion,
            run_id,
        );
        let metrics = run_metrics::run_metrics_batch(run_id, &wrote.executed.plan).unwrap();
        let batch = &metrics.batch;

        assert_eq!(u64_values(batch, "node")[0], Some(0), "{run_id}: {batch:?}");
        assert_eq!(
            string_values(batch, "operator")[0],
            "DataSinkExec",
            "{run_id}"
        );
        assert_eq!(
            u64_values(batch, "partition")[0],
            None,
            "{run_id}: {batch:?}"
        );
        assert_eq!(
            u64_values(batch, "rows_written")[0],
            Some(32),
            "{run_id}: {batch:?}"
        );
        let bytes_written = u64_values(batch, "bytes_written")[0];
        let file_size = wrote.file_size.unwrap();
        assert!(
            bytes_written.is_some_and(|bytes| bytes > 0 && bytes <= file_size),
            "{run_id}: {bytes_written:?} bytes written to a file of {file_size}"
        );
    }
}

/// A Parquet scan sums per-file metrics into the row for its one partition.
#[test]
fn a_parquet_scan_over_several_files_yields_one_row_per_partition_with_per_file_metrics_summed() {
    let wrote = write_formulation(
        FixtureFormat::Parquet,
        OutputFormat::PARQUET,
        Formulation::CombineRefsUnion,
        "metrics-parquet-scan",
    );
    let metrics = run_metrics::run_metrics_batch("scan", &wrote.executed.plan).unwrap();
    let batch = &metrics.batch;
    let nodes = u64_values(batch, "node");
    let partitions = u64_values(batch, "partition");
    let scan_rows = rows_of_operator(batch, "DataSourceExec");
    let mut scan_nodes: Vec<Option<u64>> = scan_rows.iter().map(|&row| nodes[row]).collect();
    scan_nodes.dedup();
    assert_eq!(scan_nodes.len(), SAMPLES.len(), "{batch:?}");
    for node in scan_nodes {
        let rows: Vec<usize> = scan_rows
            .iter()
            .copied()
            .filter(|&row| nodes[row] == node)
            .collect();
        let partitioned: Vec<Option<u64>> = rows.iter().map(|&row| partitions[row]).collect();
        assert_eq!(partitioned, [None, Some(0)], "node {node:?}: {batch:?}");
        let (global, scanned) = (rows[0], rows[1]);
        assert_eq!(
            u64_values(batch, "num_predicate_creation_errors")[global],
            Some(0)
        );
        assert_eq!(u64_values(batch, "files_opened")[global], None);
        let files = u64::try_from(fixture::sample_rows().len() / 2).unwrap();
        assert_eq!(u64_values(batch, "files_opened")[scanned], Some(files));
        assert_eq!(
            u64_values(batch, "row_groups_pruned_statistics_total")[scanned],
            Some(files),
            "{batch:?}"
        );
        assert_eq!(
            u64_values(batch, "row_groups_pruned_statistics_pruned")[scanned],
            Some(0)
        );
        assert_eq!(
            u64_values(batch, "files_ranges_pruned_statistics_total")[scanned],
            Some(files)
        );
        let bytes_scanned = u64_values(batch, "bytes_scanned")[scanned];
        assert!(bytes_scanned.is_some_and(|bytes| bytes > 0), "{batch:?}");
        assert_eq!(
            u64_values(batch, "scan_efficiency_ratio_num")[scanned],
            bytes_scanned
        );
        assert!(
            u64_values(batch, "metadata_load_time")[scanned].is_some_and(|nanos| nanos > 0),
            "{batch:?}"
        );
        assert_eq!(u64_values(batch, "rows_written")[scanned], None);
    }
}

/// The allele combiner records aggregate, repartition, and window metrics with none unrecorded.
#[test]
fn the_allele_combiner_records_its_aggregate_and_repartition_metrics() {
    let wrote = write_formulation(
        FixtureFormat::Vortex,
        OutputFormat::VORTEX,
        Formulation::CombineAllelesUnion,
        "metrics-alleles",
    );
    let metrics = run_metrics::run_metrics_batch("alleles", &wrote.executed.plan).unwrap();
    assert_eq!(metrics.unrecorded, Vec::<String>::new());
    let batch = &metrics.batch;

    let aggregates = rows_of_operator(batch, "AggregateExec");
    assert!(!aggregates.is_empty(), "{batch:?}");
    for row in aggregates {
        assert!(u64_values(batch, "partition")[row].is_some(), "{batch:?}");
        for column in [
            "spill_count",
            "spilled_bytes",
            "spilled_rows",
            "time_calculating_group_ids",
            "aggregation_time",
            "emitting_time",
        ] {
            assert!(
                u64_values(batch, column)[row].is_some(),
                "{column} on row {row}: {batch:?}"
            );
        }
        assert_eq!(u64_values(batch, "fetch_time")[row], None, "{batch:?}");
        assert_eq!(u64_values(batch, "peak_mem_used")[row], None, "{batch:?}");
    }
    let repartitions = rows_of_operator(batch, "RepartitionExec");
    assert!(!repartitions.is_empty(), "{batch:?}");
    assert!(
        repartitions
            .iter()
            .any(|&row| u64_values(batch, "fetch_time")[row].is_some_and(|nanos| nanos > 0)),
        "{batch:?}"
    );
    assert!(
        repartitions
            .iter()
            .all(|&row| u64_values(batch, "output_rows")[row].is_some()),
        "{batch:?}"
    );
    let windows = rows_of_operator(batch, "BoundedWindowAggExec");
    assert!(!windows.is_empty(), "{batch:?}");
    assert!(
        windows
            .iter()
            .all(|&row| u64_values(batch, "output_batches")[row].is_some()),
        "{batch:?}"
    );
}

struct FormulationWrite {
    executed: sink::ExecutedSink,
    file_size: Option<u64>,
}

fn write_formulation(
    fixture_format: FixtureFormat,
    output_format: OutputFormat,
    formulation: Formulation,
    store_name: &str,
) -> FormulationWrite {
    let input = Arc::clone(fixture::dataset_fixture(
        fixture_format,
        LocusRepresentation::ContigPosition,
    ));
    let output = MemoryStore::new(store_name);
    let extension = output_format.extension();
    let single_file = formulation.output_layout() == OutputLayout::SingleFile;
    let object_path = single_file.then(|| Path::from(format!("combined.{extension}")));
    let output_path = if single_file {
        format!("{}combined.{extension}", output.url().as_str())
    } else {
        format!("{}combined", output.url().as_str())
    };
    let target = WriteTarget {
        output_path,
        output_format,
    };

    pipeline::run(
        move |ctx| async move {
            input.register(&ctx);
            output.register(&ctx);
            let dataset = Dataset::discover(
                &ctx,
                input.table_path().clone(),
                input.input_format(),
                formulation.required_ordering(),
                None,
            )
            .await?;
            let executed = target
                .write(formulation.plan(&ctx, &dataset).await?)
                .await?;
            let file_size = match object_path {
                Some(path) => Some(output.store().head(&path).await?.size),
                None => None,
            };
            Ok(FormulationWrite {
                executed,
                file_size,
            })
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
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
