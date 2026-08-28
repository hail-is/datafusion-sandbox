//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: one sort-preserving merge over one partition per sample, and no
//! re-sort. A regression from merging to re-sorting is invisible in the results
//! and costs an order of magnitude in time, so it is asserted structurally here.

use crate::fixture;

use datafusion::{
    datasource::listing::ListingTableUrl,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::{SessionConfig, SessionContext, col},
};
use datafusion_sandbox::{
    dataset::{Dataset, DatasetLayout},
    format::InputFormat,
    formulation::Formulation,
};

use std::{future::Future, sync::Arc};

use fixture::SAMPLES;

const FORMULATIONS: [Formulation; 2] = [
    Formulation::CombineRefsUnion,
    Formulation::CombineAllelesUnion,
];

#[test]
fn rejects_a_dataset_with_an_insufficient_locus_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &[SAMPLES[0]]);
    let table_path = ListingTableUrl::parse(&root).unwrap();
    let ctx = SessionContext::new();
    let dataset = block_on(Dataset::discover(
        &ctx,
        table_path,
        InputFormat::VORTEX,
        DatasetLayout {
            locus_ordering: vec![col("contig").sort(true, false)],
            schema: None,
        },
    ))
    .unwrap();

    let error = block_on(Formulation::CombineRefsUnion.plan(&SessionContext::new(), &dataset))
        .expect_err("the formulation requires position ordering");

    assert!(error.to_string().contains("locus ordering"));
}

#[test]
fn formulations_keep_their_plan_shape_under_a_hostile_session() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);
    let dataset = dataset(&root, InputFormat::VORTEX);

    for formulation in FORMULATIONS {
        let plan = physical_plan(formulation, &dataset);
        assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
    }
}

#[test]
fn target_partitions_do_not_change_either_formulation_plan_shape() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);
    let dataset = dataset(&root, InputFormat::VORTEX);

    for formulation in FORMULATIONS {
        let single_target = physical_plan_with_target(formulation, &dataset, 1);
        let eight_targets = physical_plan_with_target(formulation, &dataset, 8);
        assert_eq!(displayed(&single_target), displayed(&eight_targets));
    }
}

/// The allele formulation still derives a single-target session for its
/// downstream distinct. That override must not reach back into the caller.
#[test]
fn allele_session_derivation_leaves_the_callers_session_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);
    let dataset = dataset(&root, InputFormat::VORTEX);
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));

    block_on(Formulation::CombineAllelesUnion.plan(&ctx, &dataset)).unwrap();

    let options = ctx.state().config().options().clone();
    assert_eq!(
        options.execution.target_partitions, 8,
        "the allele formulation overrode target_partitions on the caller's session",
    );
}

/// DataFusion's parquet reader preserves the declared locus ordering through
/// the reference combiner's union, so the requested ordering needs a merge but
/// no re-sort.
#[test]
fn combine_refs_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_parquet_sample_tables(dir.path(), SAMPLES);

    let plan = physical_plan(
        Formulation::CombineRefsUnion,
        &dataset(&root, InputFormat::PARQUET),
    );

    assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
}

/// The reference combiner's union formulation merges its per-sample inputs
/// rather than re-sorting them.
#[test]
fn combine_refs_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);

    let plan = physical_plan(
        Formulation::CombineRefsUnion,
        &dataset(&root, InputFormat::VORTEX),
    );

    assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
}

/// DataFusion's parquet reader preserves the declared locus ordering through the
/// allele combiner's union, and its de-duplication and ranking don't reintroduce
/// a sort.
#[test]
fn combine_alleles_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_parquet_sample_tables(dir.path(), SAMPLES);

    let plan = physical_plan(
        Formulation::CombineAllelesUnion,
        &dataset(&root, InputFormat::PARQUET),
    );

    assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
}

/// The allele combiner merges its per-sample inputs rather than re-sorting them,
/// and the de-duplication and ranking it stacks on top don't reintroduce a sort.
#[test]
fn combine_alleles_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);

    let plan = physical_plan(
        Formulation::CombineAllelesUnion,
        &dataset(&root, InputFormat::VORTEX),
    );

    assert_merges_one_partition_per_sample(&plan, SAMPLES.len());
}

#[test]
fn restricting_the_sample_set_changes_input_count_for_every_formulation() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), SAMPLES);
    let requested = SAMPLES[..2]
        .iter()
        .map(|sample| sample.to_string())
        .collect::<Vec<_>>();
    let dataset = dataset(&root, InputFormat::VORTEX)
        .restrict_to(&requested)
        .unwrap();
    for formulation in FORMULATIONS {
        let plan = physical_plan(formulation, &dataset);

        assert_merges_one_partition_per_sample(&plan, 2);
    }
}

fn dataset(root: &str, input_format: InputFormat) -> Dataset {
    let table_path = ListingTableUrl::parse(root).unwrap();
    let ctx = SessionContext::new();
    block_on(Dataset::discover(
        &ctx,
        table_path,
        input_format,
        DatasetLayout {
            locus_ordering: vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
                col("alleles").sort(true, false),
            ],
            schema: None,
        },
    ))
    .unwrap()
}

/// Builds a formulation under settings that would split an unpinned file scan.
/// The sorted table must keep one partition per sample even when the optimizer
/// is allowed to split files of any size.
fn physical_plan(formulation: Formulation, dataset: &Dataset) -> Arc<dyn ExecutionPlan> {
    physical_plan_with_target(formulation, dataset, 8)
}

fn physical_plan_with_target(
    formulation: Formulation,
    dataset: &Dataset,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    let mut hostile_config = SessionConfig::new().with_target_partitions(target_partitions);
    hostile_config
        .options_mut()
        .optimizer
        .repartition_file_min_size = 0;
    block_on(async {
        let ctx = SessionContext::new_with_config(hostile_config);
        formulation
            .plan(&ctx, dataset)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// Exactly one sort-preserving merge, no re-sort, and one input partition per
/// sample feeding the merge.
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

    let merges = nodes_of::<SortPreservingMergeExec>(plan);
    assert_eq!(
        merges.len(),
        1,
        "expected exactly one SortPreservingMergeExec, got {}:\n{}",
        merges.len(),
        displayed(plan),
    );
    assert!(
        nodes_of::<SortExec>(plan).is_empty(),
        "expected no re-sort, but the plan contains a SortExec:\n{}",
        displayed(plan),
    );

    let merged_partitions = merges[0]
        .children()
        .first()
        .map(|input| input.output_partitioning().partition_count())
        .expect("a SortPreservingMergeExec has an input");
    assert_eq!(
        merged_partitions,
        n_samples,
        "expected one merged partition per sample:\n{}",
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
