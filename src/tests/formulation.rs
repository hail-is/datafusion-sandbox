//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: a sort-preserving merge over one partition per sample, or one per
//! sample group with a merge per group beneath it, and no re-sort. A regression
//! from merging to re-sorting is invisible in the results and costs an order of
//! magnitude in time, so it is asserted structurally here.
//!
//! Every plan here is observed through a sink, as every action runs it: the
//! sink's ordering requirement is what holds a merge per sample group beneath
//! the final merge, so a bare frame's physical plan is not the plan a run
//! executes. See ADR 0014.
//!
//! The tests here build unfiltered plans. `filtered_plans` holds the same
//! formulations under a caller's contig or locus-interval filter, and
//! `grouped_merge` holds what is particular to merging by sample group.

mod filtered_plans;
mod grouped_merge;

use crate::fixture;

use crate::{
    dataset::{Dataset, DatasetLayout},
    format::OutputFormat,
    formulation::Formulation,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    pipeline, sink,
};
use datafusion::{
    common::DataFusionError,
    datasource::sink::DataSinkExec,
    error::Result,
    physical_expr::expressions::Column,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use std::{num::NonZeroUsize, sync::Arc};

use fixture::{FixtureFormat, SAMPLES, block_on};

/// The group count the shared loops run grouped-merge with: two groups over the fixture's four
/// samples, so every group holds more than one sample and there is more than one group.
const GROUPS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

const FORMULATIONS: [Formulation; 3] = [
    Formulation::CombineRefsUnion,
    Formulation::CombineRefsGroupedMerge { groups: GROUPS },
    Formulation::CombineAllelesUnion,
];
const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

#[test]
fn rejects_a_dataset_with_an_insufficient_locus_ordering() {
    let dataset = dataset_with_layout(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
        DatasetLayout {
            locus_ordering: LocusOrdering::locus(),
        },
    );
    let ctx = SessionContext::new();
    dataset.fixture.register(&ctx);

    let error = block_on(Formulation::CombineAllelesUnion.plan(&ctx, &dataset.dataset))
        .expect_err("the allele formulation requires alleles ordering");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("locus ordering"));
}

#[test]
fn formulations_keep_their_plan_shape_under_a_hostile_session() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in FORMULATIONS {
                let plan = physical_plan(formulation, &dataset);
                assert_merge_tree(&plan, &expected_groups(formulation, SAMPLES.len()));
            }
        }
    }
}

/// The file sink holds the same merge tree as the draining sink, and requires the formulation's
/// ordering in the dataset's representation, in both output formats.
#[test]
fn formulations_keep_their_plan_shape_through_the_file_sink() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in FORMULATIONS {
                let plan = file_sink_plan(formulation, &dataset, None);
                assert_merge_tree(&plan, &expected_groups(formulation, SAMPLES.len()));
                let sink_exec = plan.downcast_ref::<DataSinkExec>().unwrap_or_else(|| {
                    panic!(
                        "expected the plan to end in the file sink:\n{}",
                        displayed(&plan)
                    )
                });
                let required: Vec<&str> = sink_exec
                    .sort_order()
                    .as_ref()
                    .unwrap_or_else(|| panic!("expected a requirement:\n{}", displayed(&plan)))
                    .iter()
                    .map(|sort| {
                        sort.expr
                            .downcast_ref::<Column>()
                            .map_or("<not a column>", Column::name)
                    })
                    .collect();
                assert_eq!(
                    required,
                    ordering(formulation, &dataset).column_names(),
                    "{format:?} {representation:?} {formulation:?}"
                );
            }
        }
    }
}

#[test]
fn target_partitions_do_not_introduce_sorts_into_either_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in FORMULATIONS {
                let single_target = physical_plan_with_target(formulation, &dataset, 1);
                let eight_targets = physical_plan_with_target(formulation, &dataset, 8);
                assert_has_no_sorts(&single_target);
                assert_has_no_sorts(&eight_targets);
            }
        }
    }
}

/// `DataFusion`'s parquet reader preserves the declared locus ordering through
/// the reference combiner's union, so the requested ordering needs a merge but
/// no re-sort.
#[test]
fn combine_refs_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    for representation in REPRESENTATIONS {
        let plan = physical_plan(
            Formulation::CombineRefsUnion,
            &dataset(FixtureFormat::Parquet, representation),
        );
        assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
    }
}

/// The reference combiner's union formulation merges its per-sample inputs
/// rather than re-sorting them.
#[test]
fn combine_refs_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    for representation in REPRESENTATIONS {
        let plan = physical_plan(
            Formulation::CombineRefsUnion,
            &dataset(FixtureFormat::Vortex, representation),
        );
        assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
    }
}

/// `DataFusion`'s parquet reader preserves the declared locus ordering through the
/// allele combiner's union, and its de-duplication and ranking don't reintroduce
/// a sort.
#[test]
fn combine_alleles_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    for representation in REPRESENTATIONS {
        let plan = physical_plan(
            Formulation::CombineAllelesUnion,
            &dataset(FixtureFormat::Parquet, representation),
        );
        assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
    }
}

/// The allele combiner merges its per-sample inputs rather than re-sorting them,
/// and the de-duplication and ranking it stacks on top don't reintroduce a sort.
#[test]
fn combine_alleles_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    for representation in REPRESENTATIONS {
        let plan = physical_plan(
            Formulation::CombineAllelesUnion,
            &dataset(FixtureFormat::Vortex, representation),
        );
        assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
    }
}

#[test]
fn restricting_the_sample_set_changes_input_count_for_every_formulation() {
    let requested = SAMPLES[..2]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let mut dataset = dataset(format, representation);
            dataset.dataset = dataset.dataset.restrict_to(&requested).unwrap();
            for formulation in FORMULATIONS {
                let plan = physical_plan(formulation, &dataset);
                assert_merge_tree(&plan, &expected_groups(formulation, requested.len()));
            }
        }
    }
}

/// The sample groups a formulation's plan should merge over `n_samples`, as their sizes in
/// sample order. Written out rather than computed, so the test cannot agree with a wrong split.
fn expected_groups(formulation: Formulation, n_samples: usize) -> Vec<usize> {
    match (formulation, n_samples) {
        (Formulation::CombineRefsGroupedMerge { groups }, 4) if groups == GROUPS => vec![2, 2],
        (Formulation::CombineRefsGroupedMerge { groups }, 2) if groups == GROUPS => vec![1, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, n) => {
            panic!("no expected groups for {groups} groups over {n} samples")
        }
        (Formulation::CombineRefsUnion | Formulation::CombineAllelesUnion, n) => vec![n],
    }
}

struct FixtureDataset {
    fixture: &'static Arc<fixture::DatasetFixture>,
    dataset: Dataset,
    format: FixtureFormat,
}

fn dataset(format: FixtureFormat, representation: LocusRepresentation) -> FixtureDataset {
    dataset_with_layout(
        format,
        representation,
        Formulation::CombineAllelesUnion.required_layout(),
    )
}

fn dataset_with_layout(
    format: FixtureFormat,
    representation: LocusRepresentation,
    layout: DatasetLayout,
) -> FixtureDataset {
    let fixture = fixture::dataset_fixture(format, representation);
    let ctx = SessionContext::new();
    fixture.register(&ctx);
    let dataset = block_on(Dataset::discover(
        &ctx,
        fixture.table_path().clone(),
        fixture.input_format(),
        layout,
        None,
    ))
    .unwrap();
    FixtureDataset {
        fixture,
        dataset,
        format,
    }
}

/// Builds a formulation under settings that would split an unpinned file scan.
/// The sorted table must keep one partition per sample even when the optimizer
/// is allowed to split files of any size.
fn physical_plan(formulation: Formulation, dataset: &FixtureDataset) -> Arc<dyn ExecutionPlan> {
    physical_plan_with_target(formulation, dataset, 8)
}

/// The shared session settings, plus permission to split a file scan of any size across
/// `target_partitions` partitions wherever the plan lets the optimizer do so.
fn hostile_config(target_partitions: usize) -> SessionConfig {
    let mut config = pipeline::session_config().with_target_partitions(target_partitions);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    config
}

fn physical_plan_with_target(
    formulation: Formulation,
    dataset: &FixtureDataset,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    block_on(async {
        let ctx = SessionContext::new_with_config(hostile_config(target_partitions));
        dataset.fixture.register(&ctx);
        let frame = formulation.plan(&ctx, &dataset.dataset).await.unwrap();
        sink_plan(formulation, frame, dataset).await.unwrap()
    })
}

/// The ordering a run's sink requires of `formulation` over `dataset`.
fn ordering(formulation: Formulation, dataset: &FixtureDataset) -> StoredOrdering {
    dataset
        .dataset
        .query_ordering(&formulation.required_layout().locus_ordering)
        .unwrap()
}

/// The physical plan of `formulation` over `dataset` under the hostile session, with a row limit
/// of `limit` if given, run into the fixture format's file sink at an in-memory path. Planned,
/// not executed.
fn file_sink_plan(
    formulation: Formulation,
    dataset: &FixtureDataset,
    limit: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let output_format = match dataset.format {
        FixtureFormat::Parquet => OutputFormat::PARQUET,
        FixtureFormat::Vortex => OutputFormat::VORTEX,
    };
    let path = format!(
        "{}combined.{}",
        dataset.fixture.table_path().as_str(),
        output_format.extension()
    );
    block_on(async {
        let ctx = SessionContext::new_with_config(hostile_config(8));
        dataset.fixture.register(&ctx);
        let frame = formulation.plan(&ctx, &dataset.dataset).await.unwrap();
        let frame = match limit {
            Some(limit) => frame.limit(0, Some(limit)).unwrap(),
            None => frame,
        };
        output_format
            .sink_frame(frame, &path, Some(&ordering(formulation, dataset)))
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
}

/// The physical plan of `frame` run into a draining sink, which is the plan an action executes
/// up to the sink's name.
async fn sink_plan(
    formulation: Formulation,
    frame: DataFrame,
    dataset: &FixtureDataset,
) -> Result<Arc<dyn ExecutionPlan>> {
    sink::drain(frame, &ordering(formulation, dataset))?
        .create_physical_plan()
        .await
}

/// A sort-preserving merge over one input partition per sample, with no
/// re-sort. A formulation may merge again after parallel operators.
fn assert_merges_one_partition_per_sample(plan: &Arc<dyn ExecutionPlan>, n_samples: usize) {
    assert_merge_tree(plan, &[n_samples]);
}

/// The merge tree over `groups`, given as the size of each sample group in sample order.
///
/// One group is the flat shape: one union with a single-partition input per sample under one
/// sort-preserving merge. Several groups nest: the outermost union has one single-partition
/// input per group, and each group of more than one sample is its own flat shape beneath it,
/// while a group of one sample is that sample's scan. No sort appears anywhere. A formulation
/// may merge again after parallel operators above the outermost union.
fn assert_merge_tree(plan: &Arc<dyn ExecutionPlan>, groups: &[usize]) {
    let n_groups = groups.len();
    if let [n_samples] = groups {
        assert_flat_merge(plan, *n_samples);
        return;
    }
    let unions = nodes_of::<UnionExec>(plan);
    let Some(outer) = unions.first() else {
        panic!(
            "expected a union of {n_groups} sample groups:\n{}",
            displayed(plan)
        );
    };
    let inputs = outer.children();
    assert_eq!(
        inputs.len(),
        n_groups,
        "expected one union input per sample group:\n{}",
        displayed(plan),
    );
    for (input, &n_samples) in inputs.iter().zip(groups) {
        assert_eq!(
            input.output_partitioning().partition_count(),
            1,
            "expected one partition per sample group input:\n{}",
            displayed(plan),
        );
        if n_samples == 1 {
            assert!(
                nodes_of::<UnionExec>(input).is_empty()
                    && nodes_of::<SortPreservingMergeExec>(input).is_empty(),
                "expected a group of one sample to be its scan:\n{}",
                displayed(plan),
            );
        } else {
            assert_flat_merge(input, n_samples);
        }
    }
    let merged_groups = groups.iter().filter(|&&n_samples| n_samples > 1).count();
    assert_eq!(
        unions.len(),
        merged_groups.checked_add(1).unwrap(),
        "expected one UnionExec per merged sample group plus the union of groups:\n{}",
        displayed(plan),
    );
    let merges = nodes_of::<SortPreservingMergeExec>(plan);
    assert_eq!(
        merges.len(),
        merged_groups.checked_add(1).unwrap(),
        "expected one SortPreservingMergeExec per merged sample group plus the final merge:\n{}",
        displayed(plan),
    );
    assert_has_no_sorts(plan);
}

/// One union with `n_samples` single-partition inputs under one sort-preserving merge, and no
/// sort, anywhere in `plan`.
fn assert_flat_merge(plan: &Arc<dyn ExecutionPlan>, n_samples: usize) {
    let unions = nodes_of::<UnionExec>(plan);
    assert_eq!(
        unions.len(),
        1,
        "expected exactly one UnionExec, got {}:\n{}",
        unions.len(),
        displayed(plan),
    );
    assert_eq!(
        unions[0].children().len(),
        n_samples,
        "expected one union input per sample:\n{}",
        displayed(plan),
    );
    assert!(
        unions[0]
            .children()
            .iter()
            .all(|input| input.output_partitioning().partition_count() == 1),
        "expected one partition per sample input:\n{}",
        displayed(plan),
    );

    let merges = nodes_of::<SortPreservingMergeExec>(plan);
    assert_eq!(
        merges.len(),
        1,
        "expected exactly one SortPreservingMergeExec, got {}:\n{}",
        merges.len(),
        displayed(plan),
    );
    assert_has_no_sorts(plan);
}

fn assert_has_no_sorts(plan: &Arc<dyn ExecutionPlan>) {
    assert!(
        nodes_of::<SortExec>(plan).is_empty(),
        "expected no re-sort, but the plan contains a SortExec:\n{}",
        displayed(plan),
    );
}

/// Every node of type `T` in the plan, root first.
fn nodes_of<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    if plan.downcast_ref::<T>().is_some() {
        found.push(Arc::clone(plan));
    }
    for child in plan.children() {
        found.extend(nodes_of::<T>(child));
    }
    found
}

fn displayed(plan: &Arc<dyn ExecutionPlan>) -> String {
    datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string()
}

/// The operator on each line of the plan's display, root first and indented as displayed, without
/// the operator's details.
fn operators(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    displayed(plan)
        .lines()
        .map(|line| {
            line.split_once(':')
                .map_or(line, |(operator, _)| operator)
                .to_string()
        })
        .collect()
}
