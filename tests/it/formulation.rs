//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: one sort-preserving merge over one partition per sample, and no
//! re-sort. A regression from merging to re-sorting is invisible in the results
//! and costs an order of magnitude in time, so it is asserted structurally here.

use crate::fixture;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::{listing::ListingTableUrl, source::DataSourceExec},
    object_store::local::LocalFileSystem,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        filter::FilterExec,
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

const N_SAMPLES: usize = 4;

#[test]
fn rejects_a_dataset_with_an_insufficient_locus_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &[SAMPLES[0]]);
    let table_path = ListingTableUrl::parse(&root).unwrap();
    let dataset = block_on(Dataset::discover(
        &LocalFileSystem::new(),
        table_path,
        InputFormat::VORTEX,
        DatasetLayout {
            locus_ordering: vec![col("contig").sort(true, false)],
            partition_columns: vec![
                ("s".to_string(), DataType::Utf8),
                ("contig".to_string(), DataType::Utf8),
            ],
            schema: None,
        },
    ))
    .unwrap();

    let error = block_on(Formulation::CombineRefsUnion.plan(&SessionContext::new(), &dataset))
        .expect_err("the formulation requires position ordering");

    assert!(error.to_string().contains("locus ordering"));
}

#[test]
fn formulations_derive_the_session_their_plan_shape_needs() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);
    let dataset = dataset(&root, InputFormat::VORTEX);

    for formulation in [
        Formulation::CombineRefsUnion,
        Formulation::CombineRefsOneScan,
        Formulation::CombineAllelesUnion,
    ] {
        let plan = physical_plan(formulation, &dataset);
        assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
    }
}

/// Deriving a session must not reach back into the one it derived from.
/// `SessionContext::clone` shares a single `Arc<RwLock<SessionState>>`, so a
/// formulation that overrode settings on the context it was handed would leave
/// them in place for whatever ran next — one-scan's
/// `preserve_file_partitions` would silently reshape a union plan built
/// afterwards.
#[test]
fn deriving_a_session_leaves_the_callers_session_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);
    let dataset = dataset(&root, InputFormat::VORTEX);
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));

    for formulation in [
        Formulation::CombineRefsUnion,
        Formulation::CombineRefsOneScan,
        Formulation::CombineAllelesUnion,
    ] {
        block_on(formulation.plan(&ctx, &dataset)).unwrap();

        let options = ctx.state().config().options().clone();
        assert_eq!(
            options.execution.target_partitions, 8,
            "{formulation:?} overrode target_partitions on the caller's session",
        );
        assert_eq!(
            options.optimizer.preserve_file_partitions, 0,
            "{formulation:?} overrode preserve_file_partitions on the caller's session",
        );
    }
}

/// DataFusion's parquet reader preserves the declared locus ordering through
/// the reference combiner's union, so the requested ordering needs a merge but
/// no re-sort.
#[test]
fn combine_refs_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_parquet_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineRefsUnion,
        &dataset(&root, InputFormat::PARQUET),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The reference combiner's union formulation merges its per-sample inputs
/// rather than re-sorting them.
#[test]
fn combine_refs_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineRefsUnion,
        &dataset(&root, InputFormat::VORTEX),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// DataFusion's parquet reader preserves the declared locus ordering through the
/// allele combiner's union, and its de-duplication and ranking don't reintroduce
/// a sort.
#[test]
fn combine_alleles_union_parquet_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_parquet_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineAllelesUnion,
        &dataset(&root, InputFormat::PARQUET),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The allele combiner merges its per-sample inputs rather than re-sorting them,
/// and the de-duplication and ranking it stacks on top don't reintroduce a sort.
#[test]
fn combine_alleles_union_vortex_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineAllelesUnion,
        &dataset(&root, InputFormat::VORTEX),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
}

/// The one-scan reference formulation preserves every file partition from its
/// shared Vortex scan and merges them without re-sorting.
#[test]
fn combine_refs_one_scan_vortex_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineRefsOneScan,
        &dataset(&root, InputFormat::VORTEX),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
    assert_one_shared_scan(&plan);
}

/// DataFusion's parquet reader preserves every file partition from the one-scan
/// formulation's shared scan, so it too merges without re-sorting.
#[test]
fn combine_refs_one_scan_parquet_merges_one_partition_per_sample_without_re_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_parquet_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);

    let plan = physical_plan(
        Formulation::CombineRefsOneScan,
        &dataset(&root, InputFormat::PARQUET),
    );

    assert_merges_one_partition_per_sample(&plan, N_SAMPLES);
    assert_one_shared_scan(&plan);
}

#[test]
fn restricting_the_sample_set_changes_input_count_for_every_formulation() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_splittable_sample_tables(dir.path(), &SAMPLES[..N_SAMPLES]);
    let requested = SAMPLES[..2]
        .iter()
        .map(|sample| sample.to_string())
        .collect::<Vec<_>>();
    let dataset = dataset(&root, InputFormat::VORTEX)
        .restrict_to(&requested)
        .unwrap();
    for formulation in [
        Formulation::CombineRefsUnion,
        Formulation::CombineRefsOneScan,
        Formulation::CombineAllelesUnion,
    ] {
        let plan = physical_plan(formulation, &dataset);

        assert_merges_one_partition_per_sample(&plan, 2);
        if formulation == Formulation::CombineRefsOneScan {
            assert_one_shared_scan(&plan);
            assert!(
                nodes_of::<FilterExec>(&plan).is_empty(),
                "expected sample pruning at listing time:\n{}",
                displayed(&plan),
            );
        }
    }
}

fn dataset(root: &str, input_format: InputFormat) -> Dataset {
    let table_path = ListingTableUrl::parse(root).unwrap();
    block_on(Dataset::discover(
        &LocalFileSystem::new(),
        table_path,
        input_format,
        DatasetLayout {
            locus_ordering: vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
                col("alleles").sort(true, false),
            ],
            partition_columns: vec![
                ("s".to_string(), DataType::Utf8),
                ("contig".to_string(), DataType::Utf8),
            ],
            schema: Some(Arc::new(Schema::new(vec![
                Field::new("position", DataType::Int32, false),
                Field::new("alleles", DataType::Utf8, false),
            ]))),
        },
    ))
    .unwrap()
}

/// Builds a formulation under settings that would change its plan shape if it
/// accepted the caller's session unchanged.
///
/// Both settings are needed. `target_partitions` above the sample count is what
/// the derivation has to override, and `repartition_file_min_size` at zero is
/// what lets the optimizer act on it: at its 1 MiB default no fixture file is
/// big enough to be worth byte-range splitting, so every assertion below would
/// pass with the derivation deleted. `ROWS_PER_SAMPLE` is the third half of
/// this — see the note on it in `tests/fixture`.
fn physical_plan(formulation: Formulation, dataset: &Dataset) -> Arc<dyn ExecutionPlan> {
    let mut hostile_config = SessionConfig::new().with_target_partitions(8);
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

/// One scan feeding the merge rather than a union of per-sample scans.
fn assert_one_shared_scan(plan: &Arc<dyn ExecutionPlan>) {
    assert_eq!(
        nodes_of::<DataSourceExec>(plan).len(),
        1,
        "expected one shared scan:\n{}",
        displayed(plan),
    );
    assert!(
        nodes_of::<UnionExec>(plan).is_empty(),
        "expected no union of per-sample scans:\n{}",
        displayed(plan),
    );
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
