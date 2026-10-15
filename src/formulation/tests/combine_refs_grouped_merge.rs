//! How the group count shapes the merge tree, and that merging by input group returns the union
//! formulation's rows.
//!
//! The formulation suite's shared loops already hold grouped-merge to the merge tree over two
//! groups under a hostile session, with and without a filter.

use crate::formulation::{Formulation, combine_refs_grouped_merge};
use crate::ordered_frame::OrderedFrame;
use crate::stored::dataset::{Dataset, InputTable};
use crate::testing::fixture::{self, SAMPLES};
use crate::tests::{
    formulation_plans::{
        FORMATS, PlanBuilder, REPRESENTATIONS, collected_rows, dataset, drained_plan,
        file_sink_plan, loci, physical_plan,
    },
    plan_shape::PlanShape,
    support::hostile_config,
};

use datafusion::{
    error::Result, physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec,
    prelude::SessionContext,
};
use futures::{FutureExt, future::BoxFuture};

use std::num::NonZeroUsize;

/// The grouped-merge formulation's plan builder over this many input groups.
#[derive(Clone)]
struct GroupedMerge(usize);

impl PlanBuilder for GroupedMerge {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        combine_refs_grouped_merge::plan(ctx, dataset, groups(self.0)).boxed()
    }
}

#[test]
fn splits_the_input_tables_into_contiguous_count_balanced_input_groups() {
    let input_tables = ["a", "b", "c", "d", "e"].map(InputTable::single_sample);

    assert_eq!(
        combine_refs_grouped_merge::input_groups(&input_tables, groups(1)),
        [&input_tables[..]]
    );
    assert_eq!(
        combine_refs_grouped_merge::input_groups(&input_tables, groups(2)),
        [&input_tables[..3], &input_tables[3..]]
    );
    assert_eq!(
        combine_refs_grouped_merge::input_groups(&input_tables, groups(3)),
        [&input_tables[..2], &input_tables[2..4], &input_tables[4..]]
    );
    assert_eq!(
        combine_refs_grouped_merge::input_groups(&input_tables, groups(8)),
        input_tables
            .iter()
            .map(std::slice::from_ref)
            .collect::<Vec<_>>()
    );
}

/// Three groups over four samples: the first group holds two samples and is merged, and the
/// other two hold one sample each and feed the final merge directly.
#[test]
fn a_group_of_one_sample_feeds_the_final_merge_directly() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = physical_plan(&GroupedMerge(3), &dataset(format, representation));
            PlanShape::of(&plan).assert_merge_tree(&[2, 1, 1]);
        }
    }
}

/// One group, as many groups as samples, or more, is the union formulation's plan.
#[test]
fn one_group_or_one_sample_per_group_plans_as_the_union_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let union = physical_plan(&Formulation::CombineRefsUnion, &dataset);
            for groups in [1, SAMPLES.len(), 8] {
                let grouped = physical_plan(&GroupedMerge(groups), &dataset);
                let grouped_shape = PlanShape::of(&grouped);
                let union_shape = PlanShape::of(&union);
                assert_eq!(
                    grouped_shape.operators(),
                    union_shape.operators(),
                    "{format:?} {representation:?} {groups} groups:\n{grouped_shape}\n{union_shape}",
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
            let drained = drained_plan(&GroupedMerge(2), &dataset, hostile_config(8), Some(LIMIT));
            let (written, _) =
                file_sink_plan(&GroupedMerge(2), &dataset, hostile_config(8), Some(LIMIT));

            for (sink, plan) in [("draining", drained), ("file", written)] {
                let shape = PlanShape::of(&plan);
                shape.assert_merge_tree(&[2, 2]);
                let merges = shape.nodes_of::<SortPreservingMergeExec>();
                assert_eq!(merges.len(), 3, "{shape}");
                for node in &merges {
                    let merge = node
                        .downcast_ref::<SortPreservingMergeExec>()
                        .expect("nodes_of returned a node of another type");
                    assert_eq!(
                        merge.fetch(),
                        Some(LIMIT),
                        "{format:?} {representation:?} {sink} sink:\n{shape}"
                    );
                }
            }
        }
    }
}

/// Merging by input group changes the plan and nothing else: the same rows come back, in locus
/// order, in both formats and representations, whatever the group count.
#[test]
fn returns_the_union_formulations_rows_in_locus_order() {
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
            for groups in [2, 3] {
                let context = format!("{format:?} {representation:?} {groups} groups");
                let grouped = collected_rows(&GroupedMerge(groups), format, representation);
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

fn groups(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}
