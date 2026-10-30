use super::{IntervalMerge, THREE_INTERVALS, WITH_AN_EMPTY_INTERVAL};
use crate::formulation::Formulation;
use crate::pipeline::{self, PipelineOptions};
use crate::testing::{
    fixture::{self, Row, SAMPLES},
    formulation_plans::{
        FORMATS, FixtureDataset, PlanBuilder, REPRESENTATIONS, collected_rows, dataset, loci,
        output_format, output_path, planned,
    },
    session::hostile_config,
};
use crate::write::WriteTarget;

use datafusion::{arrow::record_batch::RecordBatch, datasource::listing::ListingTableUrl};
use futures::TryStreamExt;
use object_store::path::Path;

use std::{num::NonZeroUsize, sync::Arc};

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
                let merged =
                    collected_rows(&IntervalMerge::new(split_points), format, representation);
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
                let builder = IntervalMerge::new(split_points);
                let dataset = dataset(format, representation);
                let suffix = format!("-{}", split_points.replace([':', ','], "-"));
                let (rows_written, files) =
                    write_partitioned(&builder, dataset, &suffix, intervals, None);

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
            let builder = IntervalMerge::new(THREE_INTERVALS);
            let dataset = dataset(format, representation);
            let (rows_written, files) =
                write_partitioned(&builder, dataset, "-limited", 1, Some(LIMIT));

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

/// Writes `builder`'s plan over `dataset`, under a row limit of `limit` if given, to the output
/// path plus `suffix` on the fixture's in-memory store under a hostile session and two threads.
/// Reads the `file_count` files back in index order and returns the reported row count with each
/// file's path and batches. Fails if the directory holds any file but the `file_count` predicted
/// ones.
fn write_partitioned(
    builder: &impl PlanBuilder,
    dataset: FixtureDataset,
    suffix: &str,
    file_count: usize,
    limit: Option<usize>,
) -> (u64, Vec<(String, Vec<RecordBatch>)>) {
    let builder = builder.clone();
    let suffix = suffix.to_string();
    let output_format = output_format(dataset.format);
    pipeline::run(
        move |_| async move {
            let (ctx, ordered) = planned(&builder, &dataset, hostile_config(8)).await?;
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
            let layout = ordered.layout;
            let (frame, ordering) = ordered.hold(layout.assertion_scope())?;
            let rows_written = target.write(frame, &ordering, layout).await?.rows_written;

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
