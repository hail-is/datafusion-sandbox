//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: one sort-preserving merge over one partition per sample, and no
//! re-sort. A regression from merging to re-sorting is invisible in the results
//! and costs an order of magnitude in time, so it is asserted structurally here.

mod fixture;

use datafusion::{
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
    prelude::*,
};
use datafusion_sandbox::pipeline::PlanBuilder;
use datafusion_sandbox::{SAMPLES, combine_alleles, combine_refs, combiner_session_config};

use std::sync::Arc;

const N_SAMPLES: usize = 4;

/// The reference combiner merges its per-sample inputs rather than re-sorting
/// them.
#[test]
fn combine_refs_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables(dir.path(), samples);

    let plan =
        physical_plan(move |ctx| async move { combine_refs::plan(&ctx, &root, samples).await });

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The allele combiner merges its per-sample inputs rather than re-sorting them,
/// and the de-duplication and ranking it stacks on top don't reintroduce a sort.
#[test]
fn combine_alleles_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables(dir.path(), samples);

    let plan =
        physical_plan(move |ctx| async move { combine_alleles::plan(&ctx, &root, samples).await });

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// Exactly one sort-preserving merge, no re-sort, and one input partition per
/// sample feeding the merge.
fn assert_merges_one_partition_per_sample(plan: &Arc<dyn ExecutionPlan>, n_samples: usize) {
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

/// Builds `plan_builder`'s physical plan, against the same plan builder interface
/// the pipeline runs one through and under the same session config, since target
/// partitions is part of what decides plan shape.
fn physical_plan(plan_builder: impl PlanBuilder) -> Arc<dyn ExecutionPlan> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let ctx = SessionContext::new_with_config(combiner_session_config());
        plan_builder
            .build(ctx)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
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
