//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: one sort-preserving merge over one partition per sample, and no
//! re-sort. A regression from merging to re-sorting is invisible in the results
//! and costs an order of magnitude in time, so it is asserted structurally here.

use datafusion_sandbox::fixture;

use datafusion::{
    common::DataFusionError,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::SessionContext,
};
use datafusion_sandbox::{
    dataset::{Dataset, DatasetLayout},
    formulation::Formulation,
    locus::{LocusOrdering, LocusRepresentation},
    pipeline,
};

use std::sync::Arc;

use fixture::{FixtureFormat, SAMPLES, block_on};

const FORMULATIONS: [Formulation; 2] = [
    Formulation::CombineRefsUnion,
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
                assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
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
                assert_merges_one_partition_per_sample(&plan, requested.len());
            }
        }
    }
}

struct FixtureDataset {
    fixture: &'static Arc<fixture::DatasetFixture>,
    dataset: Dataset,
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
    FixtureDataset { fixture, dataset }
}

/// Builds a formulation under settings that would split an unpinned file scan.
/// The sorted table must keep one partition per sample even when the optimizer
/// is allowed to split files of any size.
fn physical_plan(formulation: Formulation, dataset: &FixtureDataset) -> Arc<dyn ExecutionPlan> {
    physical_plan_with_target(formulation, dataset, 8)
}

fn physical_plan_with_target(
    formulation: Formulation,
    dataset: &FixtureDataset,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    let mut hostile_config = pipeline::session_config().with_target_partitions(target_partitions);
    let optimizer = &mut hostile_config.options_mut().optimizer;
    optimizer.repartition_file_min_size = 0;
    block_on(async {
        let ctx = SessionContext::new_with_config(hostile_config);
        dataset.fixture.register(&ctx);
        formulation
            .plan(&ctx, &dataset.dataset)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
}

/// A sort-preserving merge over one input partition per sample, with no
/// re-sort. A formulation may merge again after parallel operators.
fn assert_merges_one_partition_per_sample(plan: &Arc<dyn ExecutionPlan>, n_samples: usize) {
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
