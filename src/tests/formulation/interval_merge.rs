//! What is particular to the reference combiner's interval-merge formulation: one merge per locus
//! interval, each over every sample's scan filtered to the interval, held by the sink above the
//! union of intervals; the partitioned file sink keeping one partition per interval; and that
//! merging by locus interval returns, and writes, the union formulation's rows.

use super::{
    FORMATS, FixtureDataset, REPRESENTATIONS, collected_batches, dataset, drained_plan,
    file_sink_plan, output_format, output_path, planned,
};
use crate::fixture::{self, FixtureFormat, Row, SAMPLES};
use crate::formulation::Formulation;
use crate::locus::LocusRepresentation;
use crate::ordered_frame::OutputLayout;
use crate::pipeline::{self, PipelineOptions};
use crate::sink::PartitionedSinkExec;
use crate::tests::{
    plan_shape::PlanShape,
    support::{hostile_config, interval_merge},
};
use crate::write::WriteTarget;

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::listing::ListingTableUrl,
    physical_plan::{
        ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec, union::UnionExec,
    },
};
use futures::TryStreamExt;
use object_store::path::Path;

use std::{num::NonZeroUsize, sync::Arc};

/// Split points cutting the fixture's eight loci into three intervals: `..chr01:3`, holding
/// chr01:1, chr01:2, and chr01:2; `chr01:3..chr02:2`, holding chr01:3, chr01:4, and chr02:1; and
/// `chr02:2..`, holding chr02:2 and chr02:3. The middle interval crosses the contig boundary.
const THREE_INTERVALS: &str = "1:3,2:2";

/// Split points whose middle interval, `chr01:5..chr02:1`, holds no fixture locus: it lies in the
/// gap between chr01:4 and chr02:1, both in file b, so every sample's scan keeps that file and
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
fn requires_the_reference_combiners_ordering_and_writes_a_file_per_partition() {
    let formulation = interval_merge(THREE_INTERVALS);
    assert_eq!(
        formulation.required_ordering(),
        Formulation::CombineRefsUnion.required_ordering()
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
                let (plan, ordering) =
                    file_sink_plan(&interval_merge(THREE_INTERVALS), &dataset, config, None);
                let shape = PlanShape::of(&plan);

                shape.assert_ends_in_sink_requiring(&ordering);
                let sink_nodes = shape.nodes_of::<PartitionedSinkExec>();
                assert_eq!(sink_nodes.len(), 1, "{context}:\n{shape}");
                let sink_exec = sink_nodes[0]
                    .downcast_ref::<PartitionedSinkExec>()
                    .expect("nodes_of returned a node of another type");
                assert_eq!(
                    plan.output_partitioning().partition_count(),
                    3,
                    "{context}:\n{shape}"
                );
                assert_eq!(sink_exec.partition_sinks().len(), 3, "{context}:\n{shape}");
                shape.assert_one_merge_per_interval(3, SAMPLES.len(), representation);
                let unions = shape.nodes_of::<UnionExec>();
                let outer_union = unions
                    .first()
                    .expect("the interval plan contains a union of intervals");
                assert!(
                    Arc::ptr_eq(plan.children()[0], outer_union),
                    "{context}: expected the union of intervals directly beneath the sink:\n{shape}"
                );
                assert!(
                    shape.nodes_of::<CoalescePartitionsExec>().is_empty(),
                    "{context}: expected no coalesce beneath the partitioned sink:\n{shape}"
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
                let config = if hostile {
                    hostile_config(8)
                } else {
                    pipeline::session_config()
                };
                let plan = drained_plan(&interval_merge(THREE_INTERVALS), &dataset, config, None);
                let shape = PlanShape::of(&plan);

                shape.assert_merge_tree(&[SAMPLES.len(); 3]);
                shape.assert_one_merge_per_interval(3, SAMPLES.len(), representation);
            }
        }
    }
}

/// A row limit promises one file, not a plan shape: the limit above the union of intervals forces
/// one partition, so the partitioned sink holds one partition sink and writes one file.
#[test]
fn a_row_limit_collapses_the_write_to_one_file() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let (plan, _) = file_sink_plan(
                &interval_merge(THREE_INTERVALS),
                &dataset,
                hostile_config(8),
                Some(3),
            );
            let shape = PlanShape::of(&plan);
            let sink_nodes = shape.nodes_of::<PartitionedSinkExec>();
            assert_eq!(
                sink_nodes.len(),
                1,
                "{format:?} {representation:?}:\n{shape}"
            );
            assert!(
                Arc::ptr_eq(&sink_nodes[0], &plan),
                "{format:?} {representation:?}: expected the partitioned sink at the plan root:\n{shape}"
            );
            let sink_exec = sink_nodes[0]
                .downcast_ref::<PartitionedSinkExec>()
                .expect("nodes_of returned a node of another type");
            assert_eq!(
                plan.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{shape}"
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

            let (two, _) =
                file_sink_plan(&interval_merge("1:3"), &dataset, hostile_config(8), None);
            let two_shape = PlanShape::of(&two);
            assert_eq!(
                two.output_partitioning().partition_count(),
                2,
                "{format:?} {representation:?}:\n{two_shape}"
            );
            two_shape.assert_one_merge_per_interval(2, SAMPLES.len(), representation);

            let (one, _) = file_sink_plan(&interval_merge(""), &dataset, hostile_config(8), None);
            let one_shape = PlanShape::of(&one);
            let sink_nodes = one_shape.nodes_of::<PartitionedSinkExec>();
            assert_eq!(
                sink_nodes.len(),
                1,
                "{format:?} {representation:?}:\n{one_shape}"
            );
            assert!(
                Arc::ptr_eq(&sink_nodes[0], &one),
                "{format:?} {representation:?}: expected the partitioned sink at the plan root:\n{one_shape}"
            );
            assert_eq!(
                one.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{one_shape}"
            );
            one_shape.assert_one_merge_per_interval(1, SAMPLES.len(), representation);
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
                let suffix = format!("-{}", split_points.replace([':', ','], "-"));
                let (rows_written, files) =
                    write_partitioned(&formulation, dataset, &suffix, intervals, None);

                assert_eq!(
                    rows_written,
                    u64::try_from(union.len()).unwrap(),
                    "{context}"
                );
                let mut written = Vec::new();
                for (index, (path, batches)) in files.iter().enumerate() {
                    let rows: Vec<Row> = batches
                        .iter()
                        .flat_map(|batch| fixture::decode_rows(batch, representation))
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

/// A limited write puts one file in the directory, holding the union formulation's first rows in
/// locus order: the limit above the union of intervals forces one partition, and the partitioned
/// sink writes a file per partition.
#[test]
fn a_limited_write_puts_one_file_in_the_directory() {
    const LIMIT: usize = 5;
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let context = format!("{format:?} {representation:?}");
            let union = collected_rows(&Formulation::CombineRefsUnion, format, representation);
            let formulation = interval_merge(THREE_INTERVALS);
            let dataset = dataset(format, representation);
            let (rows_written, files) =
                write_partitioned(&formulation, dataset, "-limited", 1, Some(LIMIT));

            assert_eq!(rows_written, u64::try_from(LIMIT).unwrap(), "{context}");
            let (path, batches) = files.first().unwrap();
            let rows: Vec<Row> = batches
                .iter()
                .flat_map(|batch| fixture::decode_rows(batch, representation))
                .collect();
            assert_eq!(rows.len(), LIMIT, "{context}: {path}: {rows:?}");
            // A limit can cut between rows sharing a locus, among which sample order is
            // unconstrained, so the rows are pinned by locus and by membership, not one by one.
            assert_eq!(loci(&rows), loci(&union[..LIMIT]), "{context}: {path}");
            for row in &rows {
                assert!(
                    union.contains(row),
                    "{context}: {path}: {row:?} is not a combined row"
                );
            }
        }
    }
}

fn loci(rows: &[Row]) -> Vec<crate::locus::Locus> {
    rows.iter().map(|(locus, _, _)| *locus).collect()
}

/// Runs `formulation` over the fixture on a hostile session and two threads into a collecting
/// sink, and returns the rows the sink received in order.
fn collected_rows(
    formulation: &Formulation,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Vec<Row> {
    let dataset = dataset(format, representation);
    let (_, batches) = collected_batches(formulation, dataset, Ok);
    batches
        .iter()
        .flat_map(|batch| fixture::decode_rows(batch, representation))
        .collect()
}

/// Writes `formulation` over `dataset`, under a row limit of `limit` if given, to the output path
/// plus `suffix` on the fixture's in-memory store under a hostile session and two threads. Reads
/// the `file_count` files back in index order and returns the reported row count with each file's
/// path and batches. Fails if the directory holds any file but the `file_count` predicted ones.
fn write_partitioned(
    formulation: &Formulation,
    dataset: FixtureDataset,
    suffix: &str,
    file_count: usize,
    limit: Option<usize>,
) -> (u64, Vec<(String, Vec<RecordBatch>)>) {
    let formulation = formulation.clone();
    let suffix = suffix.to_string();
    let output_format = output_format(dataset.format);
    pipeline::run(
        move |_| async move {
            let (ctx, ordered) = planned(&formulation, &dataset, hostile_config(8)).await?;
            let ordered = match limit {
                Some(limit) => ordered.limit(limit)?,
                None => ordered,
            };
            let schema = Arc::clone(ordered.frame.schema().inner());
            let directory = format!("{}{suffix}", output_path(&ordered, &dataset));
            let target = WriteTarget {
                output_path: directory.clone(),
                output_format,
            };
            let rows_written = target.write(ordered).await?.rows_written;

            let paths: Vec<String> = (0..file_count)
                .map(|index| {
                    target
                        .output_format
                        .partition_file_path(&directory, index, file_count)
                })
                .collect();
            let store = dataset.fixture.store();
            let mut listed: Vec<Path> = store
                .list(Some(&store_path(&directory)?))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await?;
            listed.sort();
            let predicted: Vec<Path> = paths
                .iter()
                .map(|path| store_path(path))
                .collect::<datafusion::error::Result<_>>()?;
            assert_eq!(listed, predicted, "files written to {directory}");
            let mut files = Vec::with_capacity(file_count);
            for path in paths {
                let batches = fixture::read_file(
                    &ctx,
                    &path,
                    &dataset.fixture.input_format(),
                    Some(Arc::clone(&schema)),
                )
                .await?;
                files.push((path, batches));
            }
            Ok((rows_written, files))
        },
        two_threads(),
    )
    .unwrap()
}

/// The location of `path` within its object store, as the store lists it.
fn store_path(path: &str) -> datafusion::error::Result<Path> {
    Ok(Path::from(ListingTableUrl::parse(path)?.prefix().as_ref()))
}

fn two_threads() -> PipelineOptions {
    PipelineOptions::new(NonZeroUsize::new(2).unwrap())
}
