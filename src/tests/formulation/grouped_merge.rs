//! What is particular to the reference combiner's grouped-merge formulation: how the group count
//! shapes the merge tree, and that merging by sample group returns the union formulation's rows.
//!
//! The shared loops in the parent module already hold grouped-merge to the merge tree over two
//! groups under a hostile session, with and without a filter.

use super::{
    FORMATS, REPRESENTATIONS, assert_merge_tree, collected_batches, dataset, displayed,
    drained_plan, file_sink_plan, hostile_config, nodes_of, operators,
};
use crate::fixture::{self, FixtureFormat, SAMPLES};
use crate::formulation::Formulation;
use crate::locus::LocusRepresentation;
use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec,
};

use std::num::NonZeroUsize;

#[test]
fn displays_as_grouped_merge() {
    assert_eq!(grouped_merge(3).to_string(), "grouped-merge");
}

#[test]
fn requires_the_reference_combiners_ordering() {
    assert_eq!(
        grouped_merge(3).required_ordering(),
        Formulation::CombineRefsUnion.required_ordering()
    );
}

/// Three groups over four samples: the first group holds two samples and is merged, and the
/// other two hold one sample each and feed the final merge directly.
#[test]
fn a_group_of_one_sample_feeds_the_final_merge_directly() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = super::physical_plan(&grouped_merge(3), &dataset(format, representation));
            assert_merge_tree(&plan, &[2, 1, 1]);
        }
    }
}

/// One group, as many groups as samples, or more, is the union formulation's plan.
#[test]
fn one_group_or_one_sample_per_group_plans_as_the_union_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let union = super::physical_plan(&Formulation::CombineRefsUnion, &dataset);
            for groups in [1, SAMPLES.len(), 8] {
                let grouped = super::physical_plan(&grouped_merge(groups), &dataset);
                assert_eq!(
                    operators(&grouped),
                    operators(&union),
                    "{format:?} {representation:?} {groups} groups:\n{}\n{}",
                    displayed(&grouped),
                    displayed(&union),
                );
            }
        }
    }
}

/// A row limit above the union of groups becomes a fetch on every merge, the group merges
/// included, and leaves the merge tree as it is, under the draining sink and the file sink alike.
///
/// This is a canary for how `DataFusion` treats a limit above a merge tree, not a requirement: no
/// promise is made about the shape of a limited plan. If it fails, delete it rather than restore
/// the shape.
#[test]
fn a_row_limit_becomes_a_fetch_on_every_merge() {
    const LIMIT: usize = 3;
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let drained = drained_plan(&grouped_merge(2), &dataset, hostile_config(8), Some(LIMIT));
            let (written, _) =
                file_sink_plan(&grouped_merge(2), &dataset, hostile_config(8), Some(LIMIT));

            for (sink, plan) in [("draining", drained), ("file", written)] {
                assert_merge_tree(&plan, &[2, 2]);
                let merges = nodes_of::<SortPreservingMergeExec>(&plan);
                assert_eq!(merges.len(), 3, "{}", displayed(&plan));
                for merge in &merges {
                    assert_eq!(
                        merge.fetch(),
                        Some(LIMIT),
                        "{format:?} {representation:?} {sink} sink:\n{}",
                        displayed(&plan)
                    );
                }
            }
        }
    }
}

/// Merging by sample group changes the plan and nothing else: the same rows come back, in locus
/// order, in both formats and representations, whatever the group count.
#[test]
fn returns_the_union_formulations_rows_in_locus_order() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let union = rows(&Formulation::CombineRefsUnion, format, representation);
            assert_eq!(
                union.len(),
                fixture::sample_rows()
                    .len()
                    .checked_mul(SAMPLES.len())
                    .unwrap()
            );
            for groups in [2, 3] {
                let context = format!("{format:?} {representation:?} {groups} groups");
                let grouped = rows(&grouped_merge(groups), format, representation);
                let loci = |rows: &[Row]| {
                    rows.iter()
                        .map(|(contig, position, _, _)| (contig.clone(), *position))
                        .collect::<Vec<_>>()
                };
                assert_eq!(loci(&grouped), loci(&union), "{context}");
                assert!(loci(&grouped).is_sorted(), "{context}: {grouped:?}");
                let mut grouped = grouped;
                grouped.sort();
                let mut union = union.clone();
                union.sort();
                assert_eq!(grouped, union, "{context}");
            }
        }
    }
}

fn grouped_merge(groups: usize) -> Formulation {
    Formulation::CombineRefsGroupedMerge {
        groups: NonZeroUsize::new(groups).unwrap(),
    }
}

/// A combined row: contig, position, alleles, and sample.
type Row = (String, i32, String, String);

/// Runs `formulation` over the fixture on a hostile session and two threads, so that the
/// group merges have another thread to run on, into a collecting sink, and returns the rows the
/// sink received in order.
fn rows(
    formulation: &Formulation,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Vec<Row> {
    let dataset = dataset(format, representation);
    let (_, batches) = collected_batches(formulation, dataset, Ok);
    batches
        .iter()
        .flat_map(|batch: &RecordBatch| {
            fixture::decode_loci(batch, representation)
                .into_iter()
                .zip(fixture::string_column(batch, "alleles"))
                .zip(fixture::string_column(batch, "s"))
                .map(|(((contig, position), alleles), sample)| (contig, position, alleles, sample))
        })
        .collect()
}
