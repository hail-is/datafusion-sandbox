//! What is particular to the reference combiner's interval-merge formulation: one merge per locus
//! interval, each over every sample's scan filtered to the interval, held by the sink above the
//! union of intervals; the partitioned file sink keeping one partition per interval; and that
//! merging by locus interval returns, and writes, the union formulation's rows.

use super::{
    FORMATS, FixtureDataset, REPRESENTATIONS, assert_filter_reaches_every_scan, assert_flat_merge,
    assert_merge_tree, dataset, displayed, file_sink_plan, file_sink_plan_with_config,
    hostile_config, nodes_of, ordering, output_format, output_path, sink_plan,
};
use crate::fixture::{self, FixtureFormat, SAMPLES, block_on};
use crate::format::OutputLayout;
use crate::formulation::Formulation;
use crate::locus::{LocusRepresentation, SplitPoints};
use crate::pipeline::{self, PipelineOptions};
use crate::sink::{self, PartitionedSinkExec};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::{listing::ListingTableUrl, source::DataSourceExec},
    physical_expr::expressions::Column,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        coalesce_partitions::CoalescePartitionsExec,
        filter::FilterExec,
        repartition::RepartitionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::SessionContext,
};
use futures::TryStreamExt;
use object_store::path::Path;

use std::sync::Arc;

/// Split points cutting the fixture's eight loci into three intervals: `..chr1:3`, holding
/// chr1:1, chr1:2, and chr1:2; `chr1:3..chr2:2`, holding chr1:3, chr1:4, and chr2:1; and
/// `chr2:2..`, holding chr2:2 and chr2:3. The middle interval crosses the contig boundary.
const THREE_INTERVALS: &str = "1:3,2:2";

/// Split points whose middle interval, `chr1:5..chr2:1`, holds no fixture locus: it lies in the
/// gap between chr1:4 and chr2:1, both in file b, so every sample's scan keeps that file and
/// returns no rows from it.
const WITH_AN_EMPTY_INTERVAL: &str = "1:5,2:1";

#[test]
fn displays_as_interval_merge() {
    assert_eq!(
        interval_merge(THREE_INTERVALS).to_string(),
        "interval-merge"
    );
}

#[test]
fn requires_the_reference_combiners_layout_and_writes_a_file_per_partition() {
    let formulation = interval_merge(THREE_INTERVALS);
    assert_eq!(
        formulation.required_layout().locus_ordering,
        Formulation::CombineRefsUnion
            .required_layout()
            .locus_ordering
    );
    assert_eq!(formulation.output_layout(), OutputLayout::FilePerPartition);
    for single_file in [
        Formulation::CombineRefsUnion,
        Formulation::CombineAllelesUnion,
    ] {
        assert_eq!(single_file.output_layout(), OutputLayout::SingleFile);
    }
}

/// Through the partitioned file sink, the plan is the sink over a union of one merge per
/// interval, each merge over every sample's scan, with the sink keeping one partition per
/// interval: no coalesce, sort, repartition, or filter operator anywhere, in either format and
/// representation, under the shared session and the hostile one. The sink requires the
/// formulation's ordering in the dataset's representation.
#[test]
fn writes_through_a_partitioned_sink_over_one_merge_per_interval() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for hostile in [false, true] {
                let context = format!("{format:?} {representation:?} hostile={hostile}");
                let config = if hostile {
                    hostile_config(8)
                } else {
                    pipeline::session_config()
                };
                let plan = file_sink_plan_with_config(
                    &interval_merge(THREE_INTERVALS),
                    &dataset,
                    None,
                    config,
                );

                let sink_exec = plan
                    .downcast_ref::<PartitionedSinkExec>()
                    .unwrap_or_else(|| {
                        panic!(
                            "{context}: expected the plan to end in the partitioned sink:\n{}",
                            displayed(&plan)
                        )
                    });
                assert_eq!(
                    plan.output_partitioning().partition_count(),
                    3,
                    "{context}:\n{}",
                    displayed(&plan)
                );
                assert_eq!(
                    sink_exec.partition_sinks().len(),
                    3,
                    "{context}:\n{}",
                    displayed(&plan)
                );
                let required: Vec<&str> = sink_exec
                    .ordering()
                    .unwrap_or_else(|| panic!("{context}: expected a requirement"))
                    .iter()
                    .map(|sort| {
                        sort.expr
                            .downcast_ref::<Column>()
                            .map_or("<not a column>", Column::name)
                    })
                    .collect();
                assert_eq!(
                    required,
                    ordering(&interval_merge(THREE_INTERVALS), &dataset).column_names(),
                    "{context}"
                );
                assert_one_merge_per_interval(&plan, 3, representation, &context);
                assert!(
                    plan.children()[0].downcast_ref::<UnionExec>().is_some(),
                    "{context}: expected the union of intervals directly beneath the sink:\n{}",
                    displayed(&plan)
                );
                assert!(
                    nodes_of::<CoalescePartitionsExec>(&plan).is_empty(),
                    "{context}: expected no coalesce beneath the partitioned sink:\n{}",
                    displayed(&plan)
                );
            }
        }
    }
}

/// Through a single-partition sink, the interval merges feed one more merge above their union,
/// so collect and explain see global locus order without a sort.
#[test]
fn a_single_partition_sink_merges_the_interval_merges() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for hostile in [false, true] {
                let context = format!("{format:?} {representation:?} hostile={hostile}");
                let plan = block_on(async {
                    let config = if hostile {
                        hostile_config(8)
                    } else {
                        pipeline::session_config()
                    };
                    let ctx = SessionContext::new_with_config(config);
                    dataset.fixture.register(&ctx);
                    let formulation = interval_merge(THREE_INTERVALS);
                    let frame = formulation.plan(&ctx, &dataset.dataset).await.unwrap();
                    sink_plan(&formulation, frame, &dataset).await.unwrap()
                });

                assert_merge_tree(&plan, &[SAMPLES.len(); 3]);
                assert_one_merge_per_interval(&plan, 3, representation, &context);
            }
        }
    }
}

/// The library keeps a defined behavior under a row limit, which the CLI rejects: the limit above
/// the union of intervals forces one partition, so the partitioned sink writes one file.
#[test]
fn a_row_limit_collapses_the_write_to_one_file() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let plan = file_sink_plan(&interval_merge(THREE_INTERVALS), &dataset, Some(3));
            let sink_exec = plan
                .downcast_ref::<PartitionedSinkExec>()
                .unwrap_or_else(|| panic!("{format:?} {representation:?}:\n{}", displayed(&plan)));
            assert_eq!(
                plan.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{}",
                displayed(&plan)
            );
            assert_eq!(sink_exec.partition_sinks().len(), 1);
        }
    }
}

/// With one split point there are two intervals and two files; with none, the plan is the union
/// formulation's under a sink of one partition, and a write is a directory of one file.
#[test]
fn one_split_point_gives_two_intervals_and_none_gives_one() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);

            let two = file_sink_plan(&interval_merge("1:3"), &dataset, None);
            assert_eq!(
                two.output_partitioning().partition_count(),
                2,
                "{format:?} {representation:?}:\n{}",
                displayed(&two)
            );
            assert_one_merge_per_interval(
                &two,
                2,
                representation,
                &format!("{format:?} {representation:?}"),
            );

            let one = file_sink_plan(&interval_merge(""), &dataset, None);
            assert!(
                one.downcast_ref::<PartitionedSinkExec>().is_some(),
                "{format:?} {representation:?}:\n{}",
                displayed(&one)
            );
            assert_eq!(
                one.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{}",
                displayed(&one)
            );
            assert_merge_tree(&one, &[SAMPLES.len()]);
        }
    }
}

/// Merging by locus interval changes the plan and nothing else: collected through the collecting
/// sink, the rows are the union formulation's rows in locus order, in both formats and
/// representations, with and without an empty interval.
#[test]
fn collects_the_union_formulations_rows_in_locus_order() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let union = collected_rows(&Formulation::CombineRefsUnion, format, representation);
            assert_eq!(
                union.len(),
                fixture::sample_rows()
                    .len()
                    .checked_mul(SAMPLES.len())
                    .unwrap()
            );
            for split_points in [THREE_INTERVALS, WITH_AN_EMPTY_INTERVAL, "1:3", ""] {
                let context = format!("{format:?} {representation:?} split points {split_points}");
                let merged = collected_rows(&interval_merge(split_points), format, representation);
                assert_eq!(loci(&merged), loci(&union), "{context}");
                assert!(loci(&merged).is_sorted(), "{context}: {merged:?}");
                let mut merged = merged;
                merged.sort();
                let mut union = union.clone();
                union.sort();
                assert_eq!(merged, union, "{context}");
            }
        }
    }
}

/// Written through the partitioned sink to the in-memory store, the directory holds one file per
/// interval, named by index, and the files read back in index order hold the union formulation's
/// rows in order. The row count returned is the total. An empty interval writes an empty file.
#[test]
fn writes_one_file_per_interval_holding_the_union_formulations_rows() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let union = collected_rows(&Formulation::CombineRefsUnion, format, representation);
            for (split_points, intervals, empty_intervals) in [
                (THREE_INTERVALS, 3, vec![]),
                (WITH_AN_EMPTY_INTERVAL, 3, vec![1]),
                ("", 1, vec![]),
            ] {
                let context = format!("{format:?} {representation:?} split points {split_points}");
                let formulation = interval_merge(split_points);
                let dataset = dataset(format, representation);
                let directory = format!(
                    "{}-{}",
                    output_path(&formulation, &dataset),
                    split_points.replace([':', ','], "-")
                );
                let (rows_written, files) =
                    write_partitioned(&formulation, dataset, &directory, intervals);

                assert_eq!(
                    rows_written,
                    u64::try_from(union.len()).unwrap(),
                    "{context}"
                );
                let mut written = Vec::new();
                for (index, (path, batches)) in files.iter().enumerate() {
                    let rows: Vec<Row> = batches
                        .iter()
                        .flat_map(|batch| rows(batch, representation))
                        .collect();
                    if empty_intervals.contains(&index) {
                        assert!(rows.is_empty(), "{context}: {path} holds {rows:?}");
                    } else {
                        assert!(!rows.is_empty(), "{context}: {path} is empty");
                    }
                    assert!(loci(&rows).is_sorted(), "{context}: {path}: {rows:?}");
                    written.extend(rows);
                }
                assert_eq!(loci(&written), loci(&union), "{context}");
                written.sort();
                let mut union = union.clone();
                union.sort();
                assert_eq!(written, union, "{context}");
            }
        }
    }
}

fn interval_merge(split_points: &str) -> Formulation {
    let split_points = if split_points.is_empty() {
        SplitPoints::new(Vec::new()).unwrap()
    } else {
        split_points.parse().unwrap()
    };
    Formulation::CombineRefsIntervalMerge { split_points }
}

/// The interval merges are `intervals` single-partition inputs of one union, each a merge over one
/// scan per sample with the interval's predicate pushed into every scan, and nothing else stands
/// between the scans and the sink: no filter, sort, or repartition operator anywhere. Above the
/// union of intervals there is either nothing, under the partitioned sink, or the one merge a
/// single-partition sink requires. One interval is the flat merge.
fn assert_one_merge_per_interval(
    plan: &Arc<dyn ExecutionPlan>,
    intervals: usize,
    representation: LocusRepresentation,
    context: &str,
) {
    if intervals == 1 {
        assert_flat_merge(plan, SAMPLES.len());
    } else {
        let unions = nodes_of::<UnionExec>(plan);
        let outer = unions.first().unwrap_or_else(|| {
            panic!(
                "{context}: expected a union of intervals:\n{}",
                displayed(plan)
            )
        });
        assert_eq!(
            outer.children().len(),
            intervals,
            "{context}: expected one union input per interval:\n{}",
            displayed(plan)
        );
        for input in outer.children() {
            assert_flat_merge(input, SAMPLES.len());
        }
        let merges = nodes_of::<SortPreservingMergeExec>(plan);
        let final_merges = merges.len().checked_sub(intervals).unwrap_or_else(|| {
            panic!(
                "{context}: expected a merge per interval:\n{}",
                displayed(plan)
            )
        });
        assert!(
            final_merges <= 1,
            "{context}: expected at most one merge above the union of intervals:\n{}",
            displayed(plan)
        );
        if final_merges == 1 {
            assert!(
                merges[0].children()[0]
                    .downcast_ref::<UnionExec>()
                    .is_some(),
                "{context}: expected the final merge directly above the union of intervals:\n{}",
                displayed(plan)
            );
        }
    }
    let n_scans = intervals.checked_mul(SAMPLES.len()).unwrap();
    if intervals > 1 {
        assert_filter_reaches_every_scan(plan, n_scans, representation);
    } else {
        assert_eq!(
            nodes_of::<DataSourceExec>(plan).len(),
            n_scans,
            "{context}:\n{}",
            displayed(plan)
        );
    }
    for (name, found) in [
        ("FilterExec", nodes_of::<FilterExec>(plan).len()),
        ("SortExec", nodes_of::<SortExec>(plan).len()),
        ("RepartitionExec", nodes_of::<RepartitionExec>(plan).len()),
    ] {
        assert_eq!(
            found,
            0,
            "{context}: expected no {name}:\n{}",
            displayed(plan)
        );
    }
    let unions = nodes_of::<UnionExec>(plan);
    assert!(
        unions
            .iter()
            .all(|union| union.output_partitioning().partition_count() == union.children().len()),
        "{context}: expected every union input to be one partition:\n{}",
        displayed(plan)
    );
}

/// A combined row: contig, position, alleles, and sample.
type Row = (String, i32, String, String);

fn loci(rows: &[Row]) -> Vec<(String, i32)> {
    rows.iter()
        .map(|(contig, position, _, _)| (contig.clone(), *position))
        .collect()
}

fn rows(batch: &RecordBatch, representation: LocusRepresentation) -> Vec<Row> {
    fixture::decode_loci(batch, representation)
        .into_iter()
        .zip(fixture::string_column(batch, "alleles"))
        .zip(fixture::string_column(batch, "s"))
        .map(|(((contig, position), alleles), sample)| (contig, position, alleles, sample))
        .collect()
}

/// Runs `formulation` over the fixture on a hostile session and two threads into a collecting
/// sink, and returns the rows the sink received in order.
fn collected_rows(
    formulation: &Formulation,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Vec<Row> {
    let dataset = dataset(format, representation);
    let formulation = formulation.clone();
    let batches = pipeline::run(
        move |_| async move {
            let ctx = SessionContext::new_with_config(hostile_config(8));
            dataset.fixture.register(&ctx);
            let frame = formulation.plan(&ctx, &dataset.dataset).await?;
            let (frame, collected) = sink::collect(frame, &ordering(&formulation, &dataset))?;
            frame.collect().await?;
            Ok(collected.take())
        },
        two_threads(),
    )
    .unwrap();
    batches
        .iter()
        .flat_map(|batch| rows(batch, representation))
        .collect()
}

/// Writes `formulation` over `dataset` to `directory` on the fixture's in-memory store on a
/// hostile session and two threads, and reads the `intervals` files back in index order. Returns
/// the row count the write reported and each file's path with its batches.
fn write_partitioned(
    formulation: &Formulation,
    dataset: FixtureDataset,
    directory: &str,
    intervals: usize,
) -> (u64, Vec<(String, Vec<RecordBatch>)>) {
    let formulation = formulation.clone();
    let directory = directory.to_string();
    let output_format = output_format(dataset.format);
    pipeline::run(
        move |_| async move {
            let ctx = SessionContext::new_with_config(hostile_config(8));
            dataset.fixture.register(&ctx);
            let frame = formulation.plan(&ctx, &dataset.dataset).await?;
            let schema = Arc::clone(frame.schema().inner());
            let rows_written = output_format
                .write(
                    frame,
                    &directory,
                    Some(&ordering(&formulation, &dataset)),
                    formulation.output_layout(),
                )
                .await?;

            let store = dataset.fixture.store();
            let listed: Vec<Path> = store
                .list(Some(&Path::from(
                    ListingTableUrl::parse(&directory)?.prefix().as_ref(),
                )))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await?;
            let mut files = Vec::with_capacity(intervals);
            for index in 0..intervals {
                let path = output_format.partition_file_path(&directory, index, intervals);
                let batches = fixture::read_file(
                    &ctx,
                    &path,
                    &dataset.fixture.input_format(),
                    Some(Arc::clone(&schema)),
                )
                .await?;
                files.push((path, batches));
            }
            assert_eq!(
                listed.len(),
                intervals,
                "expected one file per interval in {directory}, found {listed:?}"
            );
            Ok((rows_written, files))
        },
        two_threads(),
    )
    .unwrap()
}

fn two_threads() -> PipelineOptions {
    PipelineOptions {
        threads: 2,
        ..Default::default()
    }
}
