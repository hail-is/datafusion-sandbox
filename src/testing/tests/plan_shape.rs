//! Observing a physical plan's shape, and asserting the shapes this crate relies on.
//!
//! Merge-tree and partition assertions are checked on plans built from in-memory sources, whose
//! shape each test chooses. Scanned files and displayed predicates are checked on scans of the
//! dataset fixtures, since only file scans render them.

use crate::{
    format::OutputFormat,
    locus::{Locus, LocusRepresentation, SplitPoints},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, StoredOrdering},
    sink::{self, PartitionedSinkExec},
    stored::locus_sorted_table::LocusSortedTable,
    testing::{
        fixture::{self, FixtureFormat, block_on},
        memory_store::MemoryStore,
        plan_shape::{PlanShape, exec_names},
        session::hostile_config,
    },
    write::WriteTarget,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::memory::MemorySourceConfig,
    datasource::{
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        sink::DataSinkExec,
        source::DataSourceExec,
    },
    error::Result,
    logical_expr::{Expr, Operator},
    physical_expr::{
        LexOrdering, PhysicalExpr, PhysicalSortExpr,
        expressions::{BinaryExpr, Column, lit as physical_lit},
    },
    physical_plan::{
        ExecutionPlan, Partitioning, displayable,
        filter::FilterExec,
        repartition::RepartitionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::{DataFrame, SessionConfig, SessionContext, col, lit},
};
use object_store::path::Path;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

// Plans of in-memory sources, shaped by hand.

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
}

fn column_a() -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new("a", 0))
}

fn ascending() -> LexOrdering {
    LexOrdering::new([PhysicalSortExpr::new_default(column_a())]).unwrap()
}

/// An in-memory source of `partitions` partitions, each holding `rows`, declaring ascending
/// order of `a` if `sorted`.
fn source(rows: &[i64], partitions: usize, sorted: bool) -> Arc<dyn ExecutionPlan> {
    let batch =
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(rows.to_vec()))]).unwrap();
    let config =
        MemorySourceConfig::try_new(&vec![vec![batch]; partitions], schema(), None).unwrap();
    let config = if sorted {
        config.try_with_sort_information(vec![ascending()]).unwrap()
    } else {
        config
    };
    DataSourceExec::from_data_source(config)
}

/// A sorted, single-partition source: one input table's scan.
fn scan() -> Arc<dyn ExecutionPlan> {
    source(&[1, 2, 3], 1, true)
}

fn union(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    UnionExec::try_new(inputs).unwrap()
}

fn merge(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortPreservingMergeExec::new(ascending(), input))
}

fn sort(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(ascending(), input))
}

/// Keeps the rows whose first column exceeds 1, whatever that column is named.
fn filter(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let first = input.schema().field(0).name().clone();
    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new(&first, 0)),
        Operator::Gt,
        physical_lit(1_i64),
    ));
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

fn round_robin(input: Arc<dyn ExecutionPlan>, partitions: usize) -> RepartitionExec {
    RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(partitions)).unwrap()
}

/// The panic message of `assertion`, which must panic.
fn panic_message(assertion: impl FnOnce()) -> String {
    let payload = catch_unwind(AssertUnwindSafe(assertion)).expect_err("the assertion panics");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap()
}

// Plans of the dataset fixtures.

/// The plan that reads the fixture of `format` and `representation` as each of its first two
/// samples' locus-sorted table, keeps the rows `filter` selects in each, and merges them in locus
/// order.
fn merged_samples(
    format: FixtureFormat,
    representation: LocusRepresentation,
    filter: Option<&Expr>,
) -> Arc<dyn ExecutionPlan> {
    let fixture = fixture::dataset_fixture(format, representation);
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    fixture.register(&ctx);
    block_on(async {
        let mut frames = Vec::new();
        for sample in &fixture::SAMPLES[..2] {
            let path =
                ListingTableUrl::parse(format!("{}s={sample}/", fixture.table_path().as_str()))?;
            let frame = LocusSortedTable::open(&ctx, path, fixture.input_format())
                .await?
                .read(&ctx)?;
            frames.push(match filter {
                Some(filter) => frame.filter(filter.clone())?,
                None => frame,
            });
        }
        let [first, second] = <[DataFrame; 2]>::try_from(frames).unwrap();
        first
            .union(second)?
            .sort(
                RowOrdering::locus()
                    .expand(representation)
                    .sort_expressions(),
            )?
            .create_physical_plan()
            .await
    })
    .unwrap()
}

/// The plan of a plain listing-table read of each of `samples` from the fixture of `format`, one
/// scan per sample, with no table ordering the files.
fn listed_samples(format: FixtureFormat, samples: &[&str]) -> Arc<dyn ExecutionPlan> {
    let fixture = fixture::dataset_fixture(format, LocusRepresentation::Packed);
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    fixture.register(&ctx);
    block_on(async {
        let mut frames = Vec::new();
        for sample in samples {
            let path =
                ListingTableUrl::parse(format!("{}s={sample}/", fixture.table_path().as_str()))?;
            let config = ListingTableConfig::new(path)
                .with_listing_options(ListingOptions::new(fixture.input_format().read_format()))
                .infer_schema(&ctx.state())
                .await?;
            frames.push(ctx.read_table(Arc::new(ListingTable::try_new(config)?))?);
        }
        let mut frames = frames.into_iter();
        let first = frames.next().unwrap();
        frames
            .try_fold(first, DataFrame::union)?
            .create_physical_plan()
            .await
    })
    .unwrap()
}

/// The fixture's files of `sample`, in path order.
fn sample_paths(format: FixtureFormat, sample: &str) -> Vec<Path> {
    let fixture = fixture::dataset_fixture(format, LocusRepresentation::Packed);
    block_on(fixture.sample_files(sample))
        .into_iter()
        .map(|file| file.location)
        .collect()
}

const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

// Observers.

#[test]
fn display_renders_the_indented_plan() {
    let plan = merge(union(vec![scan(), scan()]));

    assert_eq!(
        PlanShape::of(&plan).to_string(),
        displayable(plan.as_ref()).indent(true).to_string()
    );
}

#[test]
fn nodes_of_returns_every_node_of_the_type_root_first() {
    let inner = union(vec![scan(), scan()]);
    let outer = union(vec![merge(Arc::clone(&inner)), scan()]);
    let plan = merge(Arc::clone(&outer));
    let shape = PlanShape::of(&plan);

    let unions = shape.nodes_of::<UnionExec>();

    assert_eq!(unions.len(), 2);
    assert!(Arc::ptr_eq(&unions[0], &outer));
    assert!(Arc::ptr_eq(&unions[1], &inner));
    assert_eq!(shape.nodes_of::<DataSourceExec>().len(), 3);
    assert!(shape.nodes_of::<SortExec>().is_empty());
}

#[test]
fn output_rows_of_sums_the_rows_every_node_of_the_type_output_once_executed() {
    // Each filter keeps the rows above 1: two of each of the first source's two partitions, and
    // one of the second source's.
    let plan = union(vec![
        filter(source(&[1, 2, 3], 2, false)),
        filter(source(&[0, 5], 1, false)),
    ]);
    let before = PlanShape::of(&plan).output_rows_of::<FilterExec>();

    let executed = Arc::clone(&plan);
    pipeline::run(
        move |ctx| async move {
            datafusion::physical_plan::collect(executed, ctx.task_ctx()).await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
    let shape = PlanShape::of(&plan);

    assert_eq!(before, 0);
    assert_eq!(shape.output_rows_of::<FilterExec>(), 5);
    assert_eq!(shape.output_rows_of::<SortExec>(), 0);
}

#[test]
fn operators_lists_each_rendered_line_with_its_indentation_and_without_details() {
    let plan = merge(union(vec![scan(), scan()]));

    assert_eq!(
        PlanShape::of(&plan).operators(),
        [
            "SortPreservingMergeExec",
            "  UnionExec",
            "    DataSourceExec",
            "    DataSourceExec"
        ]
    );
}

#[test]
fn scanned_files_lists_each_plain_scans_files_in_plan_order() {
    for format in FORMATS {
        let plan = listed_samples(format, &fixture::SAMPLES[..2]);

        let scanned = PlanShape::of(&plan).scanned_files();

        assert_eq!(
            scanned,
            [
                sample_paths(format, fixture::SAMPLES[0]),
                sample_paths(format, fixture::SAMPLES[1])
            ],
            "{format:?}:\n{}",
            PlanShape::of(&plan)
        );
    }
}

#[test]
fn scanned_files_lists_a_sorted_table_scans_files_in_its_scan_order() {
    for format in FORMATS {
        let fixture = fixture::sorted_table_fixture(
            &format!("plan-shape-sorted-{format:?}").to_lowercase(),
            format,
            LocusRepresentation::Packed,
            vec![
                vec![Locus::new(1, 1).unwrap()],
                vec![Locus::new(1, 2).unwrap()],
            ],
        );
        let ctx = SessionContext::new_with_config(pipeline::session_config());
        fixture.register(&ctx);
        let plan = block_on(async {
            LocusSortedTable::open(&ctx, fixture.table_path().clone(), fixture.input_format())
                .await?
                .read(&ctx)?
                .create_physical_plan()
                .await
        })
        .unwrap();

        let scanned = PlanShape::of(&plan).scanned_files();

        // The fixture names its files in reverse of locus order, which the scan follows.
        assert_eq!(
            scanned,
            [vec![
                fixture.file_path(0).prefix().clone(),
                fixture.file_path(1).prefix().clone()
            ]],
            "{format:?}:\n{}",
            PlanShape::of(&plan)
        );
    }
}

#[test]
fn scanned_files_lists_a_split_file_once_per_byte_range_without_the_range() {
    let file = sample_paths(FixtureFormat::Parquet, fixture::SAMPLES[0])
        .into_iter()
        .next()
        .unwrap();
    let split = listed_one_file(&file)
        .repartitioned(3, hostile_config(3).options())
        .unwrap()
        .expect("the hostile config splits a scan");
    let shape = PlanShape::of(&split);
    assert!(
        shape.to_string().contains(&format!("{file}:0..")),
        "{shape}"
    );

    assert_eq!(
        shape.scanned_files(),
        [vec![file.clone(), file.clone(), file]]
    );
}

/// The plan of a plain read of the one Parquet fixture file at `file`.
fn listed_one_file(file: &Path) -> Arc<dyn ExecutionPlan> {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let ctx = SessionContext::new();
    fixture.register(&ctx);
    let url = format!("{}{file}", fixture.table_path().object_store().as_str());
    block_on(async {
        ctx.read_parquet(url, datafusion::prelude::ParquetReadOptions::default())
            .await?
            .create_physical_plan()
            .await
    })
    .unwrap()
}

#[test]
#[should_panic(expected = "expected DataSourceExec to render its file groups")]
fn scanned_files_panics_on_a_scan_of_no_files() {
    drop(PlanShape::of(&scan()).scanned_files());
}

#[test]
fn files_in_only_scan_lists_the_files_of_a_plans_one_scan() {
    let plan = listed_samples(FixtureFormat::Parquet, &fixture::SAMPLES[..1]);

    assert_eq!(
        PlanShape::of(&plan).files_in_only_scan(),
        sample_paths(FixtureFormat::Parquet, fixture::SAMPLES[0])
    );
}

#[test]
#[should_panic(expected = "expected exactly one DataSourceExec, got 2")]
fn files_in_only_scan_panics_on_a_plan_of_two_scans() {
    let plan = listed_samples(FixtureFormat::Parquet, &fixture::SAMPLES[..2]);

    drop(PlanShape::of(&plan).files_in_only_scan());
}

#[test]
fn exec_names_reads_operator_names_from_explain_text_flat() {
    let explained = "\
+---------------+------------------------------------------+
| plan_type     | plan                                     |
+---------------+------------------------------------------+
| physical_plan | SortPreservingMergeExec: [a@0 ASC]       |
|               |   UnionExec                              |
|               |     DataSourceExec: partitions=1         |
+---------------+------------------------------------------+";

    assert_eq!(
        exec_names(explained),
        ["SortPreservingMergeExec", "UnionExec", "DataSourceExec"]
    );
}

#[test]
fn exec_names_of_a_rendered_plan_are_its_operators_unindented() {
    let plan = merge(union(vec![filter(scan()), scan()]));
    let shape = PlanShape::of(&plan);
    let rendered = shape.to_string();

    let operators = shape.operators();
    assert_eq!(
        exec_names(&rendered),
        operators
            .iter()
            .map(|operator| operator.trim())
            .collect::<Vec<_>>()
    );
}

// Merge trees.

#[test]
fn assert_merge_tree_accepts_one_group_as_a_flat_merge_of_single_partition_inputs() {
    PlanShape::of(&merge(union(vec![scan(), scan(), scan()]))).assert_merge_tree(&[3]);
}

#[test]
fn assert_merge_tree_accepts_several_groups_each_merged_unless_it_is_one_scan() {
    let plan = merge(union(vec![
        merge(union(vec![scan(), scan()])),
        scan(),
        merge(union(vec![scan(), scan(), scan()])),
    ]));

    PlanShape::of(&plan).assert_merge_tree(&[2, 1, 3]);
}

#[test]
fn assert_merge_tree_accepts_parallel_operators_between_the_final_merge_and_the_groups() {
    let plan = merge(filter(union(vec![
        merge(union(vec![scan(), scan()])),
        scan(),
    ])));

    PlanShape::of(&plan).assert_merge_tree(&[2, 1]);
}

#[test]
fn assert_merge_tree_rejects_other_shapes() {
    let two_partitions = source(&[1, 2], 2, true);
    let grouped = || merge(union(vec![merge(union(vec![scan(), scan()])), scan()]));
    for (plan, groups, expected) in [
        (
            merge(union(vec![scan(), scan()])),
            vec![3],
            "expected one union input per sample",
        ),
        (
            merge(union(vec![scan(), two_partitions])),
            vec![2],
            "expected one partition per sample input",
        ),
        (
            merge(union(vec![sort(scan()), scan()])),
            vec![2],
            "expected no re-sort",
        ),
        (
            union(vec![scan(), scan()]),
            vec![2],
            "expected exactly one SortPreservingMergeExec",
        ),
        (
            grouped(),
            vec![2, 1, 1],
            "expected one union input per input group",
        ),
        (
            grouped(),
            vec![1, 2],
            "expected a group of one input table to be its scan",
        ),
        (
            grouped(),
            vec![2, 2],
            "expected exactly one UnionExec, got 0",
        ),
        (
            merge(grouped()),
            vec![2, 1],
            "expected one SortPreservingMergeExec per merged input group plus the final merge",
        ),
        (sort(grouped()), vec![2, 1], "expected no re-sort"),
    ] {
        let message = panic_message(|| PlanShape::of(&plan).assert_merge_tree(&groups));

        assert!(message.contains(expected), "{groups:?}: {message}");
    }
}

#[test]
fn a_failed_check_within_a_child_plan_shows_the_whole_plan_and_the_subtree() {
    let group = merge(union(vec![scan(), sort(scan())]));
    let plan = merge(union(vec![Arc::clone(&group), scan()]));

    let message = panic_message(|| PlanShape::of(&plan).assert_merge_tree(&[2, 1]));

    assert!(
        message.contains(&format!(
            "whole plan:\n{}\nsubtree:\n{}",
            PlanShape::of(&plan),
            PlanShape::of(&group)
        )),
        "{message}"
    );
}

#[test]
fn a_failed_check_of_the_whole_plan_shows_it() {
    let plan = merge(union(vec![scan(), sort(scan())]));

    let message = panic_message(|| PlanShape::of(&plan).assert_no_sorts());

    assert!(
        message.ends_with(&PlanShape::of(&plan).to_string()),
        "{message}"
    );
    assert!(!message.contains("subtree:"), "{message}");
}

#[test]
fn assert_one_ordered_partition_per_sample_accepts_a_union_of_ordered_single_partitions() {
    PlanShape::of(&union(vec![scan(), scan(), scan()])).assert_one_ordered_partition_per_sample(3);
}

#[test]
fn assert_one_ordered_partition_per_sample_rejects_other_shapes() {
    for (plan, expected) in [
        (
            union(vec![scan(), scan()]),
            "expected one union input per sample",
        ),
        (
            union(vec![scan(), scan(), source(&[1], 1, false)]),
            "expected each sample input to carry an ordering",
        ),
        (
            union(vec![scan(), scan(), source(&[1], 2, true)]),
            "expected one partition per sample input",
        ),
        (
            merge(union(vec![scan(), scan(), scan()])),
            "expected no merge above the per-sample partitions",
        ),
        (
            union(vec![scan(), scan(), sort(scan())]),
            "expected no re-sort",
        ),
        (
            union(vec![union(vec![scan(), scan()]), scan()]),
            "expected exactly one UnionExec",
        ),
    ] {
        let message =
            panic_message(|| PlanShape::of(&plan).assert_one_ordered_partition_per_sample(3));

        assert!(message.contains(expected), "{message}");
    }
}

#[test]
fn assert_no_sorts_accepts_a_plan_without_a_sort() {
    PlanShape::of(&merge(union(vec![scan(), scan()]))).assert_no_sorts();
}

#[test]
#[should_panic(expected = "expected no re-sort, but the plan contains a SortExec")]
fn assert_no_sorts_rejects_a_sort_anywhere_in_the_plan() {
    PlanShape::of(&merge(union(vec![scan(), sort(scan())]))).assert_no_sorts();
}

// Filters.

#[test]
fn assert_filter_reaches_every_scan_accepts_scans_displaying_a_locus_predicate() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = merged_samples(
                format,
                representation,
                Some(&fixture::contig_filter(representation, "chr01")),
            );

            PlanShape::of(&plan).assert_filter_reaches_every_scan(2, representation);
        }
    }
}

#[test]
fn assert_filter_reaches_every_scan_rejects_scans_without_a_locus_predicate() {
    for format in FORMATS {
        for (filter, expected) in [
            (None, "expected a filter on locus to reach the scan"),
            (
                Some(col("alleles").eq(lit("A,G"))),
                "expected a filter on locus to reach the scan",
            ),
        ] {
            let plan = merged_samples(format, LocusRepresentation::Packed, filter.as_ref());

            let message = panic_message(|| {
                PlanShape::of(&plan)
                    .assert_filter_reaches_every_scan(2, LocusRepresentation::Packed);
            });

            assert!(message.contains(expected), "{format:?}: {message}");
        }
    }
}

#[test]
#[should_panic(expected = "expected 3 scans, got 2")]
fn assert_filter_reaches_every_scan_rejects_another_number_of_scans() {
    let representation = LocusRepresentation::Packed;
    let plan = merged_samples(
        FixtureFormat::Parquet,
        representation,
        Some(&fixture::contig_filter(representation, "chr01")),
    );

    PlanShape::of(&plan).assert_filter_reaches_every_scan(3, representation);
}

#[test]
fn assert_filter_stays_inside_the_scans_accepts_a_merge_of_filtered_scans() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = merged_samples(
                format,
                representation,
                Some(&fixture::contig_filter(representation, "chr01")),
            );

            PlanShape::of(&plan).assert_filter_stays_inside_the_scans(&[2]);
        }
    }
}

#[test]
fn assert_filter_stays_inside_the_scans_accepts_an_order_preserving_repartition_above_the_union() {
    let plan = merge(Arc::new(
        round_robin(union(vec![scan(), scan()]), 4).with_preserve_order(),
    ));

    PlanShape::of(&plan).assert_filter_stays_inside_the_scans(&[2]);
}

#[test]
fn assert_filter_stays_inside_the_scans_rejects_a_filter_or_repartition_outside_them() {
    for (plan, expected) in [
        (
            filter(merge(union(vec![scan(), scan()]))),
            "the plan contains a FilterExec",
        ),
        (
            merge(union(vec![Arc::new(round_robin(scan(), 1)), scan()])),
            "expected no repartition beneath a union",
        ),
        (
            merge(Arc::new(round_robin(union(vec![scan(), scan()]), 4))),
            "expected every repartition above the union to preserve its input order",
        ),
        (
            merge(union(vec![scan(), sort(scan())])),
            "expected no re-sort",
        ),
    ] {
        let message =
            panic_message(|| PlanShape::of(&plan).assert_filter_stays_inside_the_scans(&[2]));

        assert!(message.contains(expected), "{message}");
    }
}

// Interval merges.

/// One merge of the first two samples per locus interval of the split points `1:3`, each under
/// the interval's filter, beneath one union.
fn union_of_interval_merges(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Arc<dyn ExecutionPlan> {
    let split_points = SplitPoints::new(vec![Locus::new(1, 3).unwrap()]).unwrap();
    union(
        split_points
            .intervals()
            .into_iter()
            .map(|interval| {
                merged_samples(
                    format,
                    representation,
                    interval.filter(representation).as_ref(),
                )
            })
            .collect(),
    )
}

#[test]
fn assert_one_merge_per_interval_accepts_one_flat_merge_for_one_interval() {
    PlanShape::of(&merge(union(vec![scan(), scan()]))).assert_one_merge_per_interval(
        1,
        2,
        LocusRepresentation::Packed,
    );
}

#[test]
fn assert_one_merge_per_interval_accepts_filtered_merges_with_at_most_one_final_merge() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let intervals = union_of_interval_merges(format, representation);

            for plan in [Arc::clone(&intervals), merge(intervals)] {
                PlanShape::of(&plan).assert_one_merge_per_interval(2, 2, representation);
            }
        }
    }
}

#[test]
fn assert_one_merge_per_interval_rejects_other_shapes() {
    let representation = LocusRepresentation::Packed;
    let intervals = || union_of_interval_merges(FixtureFormat::Parquet, representation);
    let unfiltered = || {
        union(vec![
            merged_samples(FixtureFormat::Parquet, representation, None),
            merged_samples(FixtureFormat::Parquet, representation, None),
        ])
    };
    for (plan, n_intervals, expected) in [
        (
            merge(union(vec![scan(), scan()])),
            0,
            "expected at least one interval",
        ),
        (intervals(), 3, "expected one union input per interval"),
        (
            merge(merge(intervals())),
            2,
            "expected at most one merge above the union of intervals",
        ),
        (
            merge(filter(intervals())),
            2,
            "expected the final merge directly above the union of intervals",
        ),
        (
            unfiltered(),
            2,
            "expected a filter on locus to reach the scan",
        ),
        (filter(intervals()), 2, "expected no FilterExec"),
        (sort(merge(intervals())), 2, "expected no SortExec"),
        (
            Arc::new(round_robin(merge(intervals()), 1)),
            2,
            "expected no RepartitionExec",
        ),
    ] {
        let message = panic_message(|| {
            PlanShape::of(&plan).assert_one_merge_per_interval(n_intervals, 2, representation);
        });

        assert!(message.contains(expected), "{n_intervals}: {message}");
    }
}

// Sinks.

/// A frame of one row of `locus` and `s`, in a session with a memory store registered for
/// writes, ordered by `ordering` and laid out by `layout`.
fn ordered_frame(ordering: StoredOrdering, layout: OutputLayout) -> OrderedFrame {
    let ctx = SessionContext::new();
    MemoryStore::new("plan-shape-sinks").register(&ctx);
    let locus: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let sample: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let batch = RecordBatch::try_from_iter([("locus", locus), ("s", sample)]).unwrap();
    OrderedFrame {
        frame: ctx.read_batch(batch).unwrap(),
        ordering,
        layout,
    }
}

fn physical(frame: Result<DataFrame>) -> Arc<dyn ExecutionPlan> {
    block_on(frame.unwrap().create_physical_plan()).unwrap()
}

fn locus() -> StoredOrdering {
    RowOrdering::locus().expand(LocusRepresentation::Packed)
}

fn locus_then_sample() -> StoredOrdering {
    RowOrdering::locus_then_sample().expand(LocusRepresentation::Packed)
}

#[test]
fn assert_ends_in_sink_requiring_accepts_a_file_or_partitioned_sink_requiring_the_ordering() {
    for ordering in [locus(), locus_then_sample()] {
        let drained = physical(sink::drain(ordered_frame(
            ordering.clone(),
            OutputLayout::SingleFile,
        )));
        let partitioned = physical(
            WriteTarget {
                output_path: "memory://plan-shape-sinks/out".to_string(),
                output_format: OutputFormat::PARQUET,
            }
            .sink_frame(ordered_frame(
                ordering.clone(),
                OutputLayout::FilePerPartition,
            )),
        );
        assert!(drained.is::<DataSinkExec>());
        assert!(partitioned.is::<PartitionedSinkExec>());

        for plan in [drained, partitioned] {
            PlanShape::of(&plan).assert_ends_in_sink_requiring(&ordering);
        }
    }
}

#[test]
fn assert_ends_in_sink_requiring_rejects_another_root_or_ordering() {
    let drained = physical(sink::drain(ordered_frame(
        locus_then_sample(),
        OutputLayout::SingleFile,
    )));
    for (plan, expected) in [
        (
            merge(union(vec![scan(), scan()])),
            "expected the plan to end in DataSinkExec or PartitionedSinkExec",
        ),
        (drained, "expected the sink to require the stored ordering"),
    ] {
        let message =
            panic_message(|| PlanShape::of(&plan).assert_ends_in_sink_requiring(&locus()));

        assert!(message.contains(expected), "{message}");
    }
}
