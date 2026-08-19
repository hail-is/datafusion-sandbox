//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: one sort-preserving merge over one partition per sample, and no
//! re-sort. A regression from merging to re-sorting is invisible in the results
//! and costs an order of magnitude in time, so it is asserted structurally here.

mod fixture;

use datafusion::{
    arrow::datatypes::DataType,
    datasource::source::DataSourceExec,
    error::Result,
    logical_expr::{LogicalPlan, SortExpr, logical_plan::Union},
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::*,
};
use datafusion_sandbox::{
    SAMPLES, combine_alleles, combine_refs, combine_refs_one_scan, combiner_session_config,
};

use std::{future::Future, sync::Arc};

const N_SAMPLES: usize = 4;

/// DataFusion's parquet reader preserves the declared locus ordering through
/// the reference combiner's union, so the requested ordering needs a merge but
/// no re-sort.
#[test]
fn parquet_read_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables_with_format(
        dir.path(),
        samples,
        fixture::FixtureFileFormat::Parquet,
        "fixture.parquet",
    );

    let plan = physical_plan(combiner_session_config(), move |ctx| async move {
        parquet_reference_plan(&ctx, &root, samples).await
    });

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The reference combiner merges its per-sample inputs rather than re-sorting
/// them.
#[test]
fn combine_refs_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables(dir.path(), samples);

    let plan = physical_plan(combiner_session_config(), move |ctx| async move {
        combine_refs::plan(&ctx, &root, samples).await
    });

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The allele combiner merges its per-sample inputs rather than re-sorting them,
/// and the de-duplication and ranking it stacks on top don't reintroduce a sort.
#[test]
fn combine_alleles_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables(dir.path(), samples);

    let plan = physical_plan(combiner_session_config(), move |ctx| async move {
        combine_alleles::plan(&ctx, &root, samples).await
    });

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The earlier reference combiner variant preserves every file partition from
/// its single shared scan and merges them without re-sorting.
#[test]
fn combine_refs_one_scan_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let samples = &SAMPLES[..N_SAMPLES];
    let root = fixture::write_sample_tables(dir.path(), samples);

    let plan = physical_plan(
        combine_refs_one_scan::session_config(),
        move |ctx| async move { combine_refs_one_scan::plan(&ctx, &root).await },
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
    assert_eq!(
        nodes_of::<DataSourceExec>(&plan).len(),
        1,
        "expected one shared scan:\n{}",
        displayed(&plan),
    );
    assert!(
        nodes_of::<UnionExec>(&plan).is_empty(),
        "expected no union of per-sample scans:\n{}",
        displayed(&plan),
    );
}

// TODO(#27): Delete this copy when the plan builders take the input format
// type. The parquet test should call `combine_refs::plan` after that change.
async fn parquet_reference_plan(
    ctx: &SessionContext,
    table_path: &str,
    samples: &[&str],
) -> Result<DataFrame> {
    let locus_ordering = locus_ordering();
    let read_options = ParquetReadOptions::default()
        .table_partition_cols(vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ])
        .file_sort_order(vec![locus_ordering.clone()]);
    let df = ctx
        .read_parquet(
            format!("{}/", table_path.trim_end_matches('/')),
            read_options,
        )
        .await?;

    let plans = samples
        .iter()
        .map(|sample| {
            Ok(Arc::new(
                df.clone()
                    .filter(col("s").eq(lit(*sample)))?
                    .into_unoptimized_plan(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let plan = LogicalPlan::Union(Union::try_new(plans)?);

    DataFrame::new(ctx.state(), plan).sort(locus_ordering)
}

fn locus_ordering() -> Vec<SortExpr> {
    vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ]
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

/// Builds `plan_builder`'s physical plan under the given session config, since
/// target partitions is part of what decides plan shape. Plan-shape assertions
/// deliberately stop at the plan builder, before execution.
fn physical_plan<F, Fut>(session_config: SessionConfig, plan_builder: F) -> Arc<dyn ExecutionPlan>
where
    F: FnOnce(SessionContext) -> Fut,
    Fut: Future<Output = Result<DataFrame>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let ctx = SessionContext::new_with_config(session_config);
        plan_builder(ctx)
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
