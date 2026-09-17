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
//! The tests here build unfiltered plans of the single-file formulations.
//! `filtered_plans` holds the same formulations under a caller's contig or
//! locus-interval filter, `grouped_merge` holds what is particular to merging
//! by sample group, and `interval_merge` holds the interval-merge formulation,
//! whose plan has one scan per sample per locus interval and whose write ends
//! in the partitioned sink rather than the file sink.

mod filtered_plans;
mod grouped_merge;
mod interval_merge;

use crate::fixture;

use crate::{
    dataset::Dataset,
    format::OutputFormat,
    formulation::Formulation,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    sink,
};
use datafusion::{
    arrow::record_batch::RecordBatch,
    common::DataFusionError,
    datasource::{sink::DataSinkExec, source::DataSourceExec},
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

/// The formulations the shared loops cover: every one that writes a single file. A function
/// rather than a constant because a formulation may hold split points, so it is not `Copy`.
fn formulations() -> [Formulation; 3] {
    [
        Formulation::CombineRefsUnion,
        Formulation::CombineRefsGroupedMerge { groups: GROUPS },
        Formulation::CombineAllelesUnion,
    ]
}
const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

#[test]
fn rejects_a_dataset_with_an_insufficient_locus_ordering() {
    let dataset = dataset_with_ordering(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
        LocusOrdering::locus(),
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
            for formulation in &formulations() {
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
            for formulation in &formulations() {
                let (plan, ordering) =
                    file_sink_plan(formulation, &dataset, hostile_config(8), None);
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
                    ordering.column_names(),
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
            for formulation in &formulations() {
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
            &Formulation::CombineRefsUnion,
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
            &Formulation::CombineRefsUnion,
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
            &Formulation::CombineAllelesUnion,
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
            &Formulation::CombineAllelesUnion,
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
            for formulation in &formulations() {
                let plan = physical_plan(formulation, &dataset);
                assert_merge_tree(&plan, &expected_groups(formulation, requested.len()));
            }
        }
    }
}

/// The sample groups a formulation's plan should merge over `n_samples`, as their sizes in
/// sample order. Written out rather than computed, so the test cannot agree with a wrong split.
/// Interval-merge merges every sample once per locus interval; `interval_merge` spells that out.
fn expected_groups(formulation: &Formulation, n_samples: usize) -> Vec<usize> {
    match (formulation, n_samples) {
        (Formulation::CombineRefsGroupedMerge { groups }, 4) if *groups == GROUPS => vec![2, 2],
        (Formulation::CombineRefsGroupedMerge { groups }, 2) if *groups == GROUPS => vec![1, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, n) => {
            panic!("no expected groups for {groups} groups over {n} samples")
        }
        (Formulation::CombineRefsIntervalMerge { split_points }, n) => {
            panic!("no expected groups for split points {split_points} over {n} samples")
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
    dataset_with_ordering(
        format,
        representation,
        Formulation::CombineAllelesUnion.required_ordering(),
    )
}

fn dataset_with_ordering(
    format: FixtureFormat,
    representation: LocusRepresentation,
    ordering: LocusOrdering,
) -> FixtureDataset {
    let fixture = fixture::dataset_fixture(format, representation);
    let ctx = SessionContext::new();
    fixture.register(&ctx);
    let dataset = block_on(Dataset::discover(
        &ctx,
        fixture.table_path().clone(),
        fixture.input_format(),
        ordering,
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
fn physical_plan(formulation: &Formulation, dataset: &FixtureDataset) -> Arc<dyn ExecutionPlan> {
    drained_plan(formulation, dataset, hostile_config(8), None)
}

/// The shared session settings, plus permission to split a file scan of any size across
/// `target_partitions` partitions wherever the plan lets the optimizer do so.
fn hostile_config(target_partitions: usize) -> SessionConfig {
    let mut config = pipeline::session_config().with_target_partitions(target_partitions);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    config
}

fn physical_plan_with_target(
    formulation: &Formulation,
    dataset: &FixtureDataset,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    drained_plan(
        formulation,
        dataset,
        hostile_config(target_partitions),
        None,
    )
}

/// The fixture format as an output format, so a plan writes what it read.
const fn output_format(format: FixtureFormat) -> OutputFormat {
    match format {
        FixtureFormat::Parquet => OutputFormat::PARQUET,
        FixtureFormat::Vortex => OutputFormat::VORTEX,
    }
}

/// The in-memory path a write of `formulation` over `dataset` goes to: a file named with the
/// output format's extension, or a directory for a formulation writing one file per partition.
fn output_path(ordered: &OrderedFrame, dataset: &FixtureDataset) -> String {
    let root = dataset.fixture.table_path().as_str().trim_end_matches('/');
    match ordered.layout {
        OutputLayout::SingleFile => format!(
            "{root}/combined.{}",
            output_format(dataset.format).extension()
        ),
        OutputLayout::FilePerPartition => format!("{root}/combined"),
    }
}

/// The physical plan of `formulation` over `dataset` under `config`, with a row limit of `limit`
/// if given, run into the fixture format's file sink at an in-memory path. Planned, not executed.
/// Returns the ordering carried by the frame so the sink requirement can be checked against it.
fn file_sink_plan(
    formulation: &Formulation,
    dataset: &FixtureDataset,
    config: SessionConfig,
    limit: Option<usize>,
) -> (Arc<dyn ExecutionPlan>, StoredOrdering) {
    block_on(async {
        let (_, ordered) = planned(formulation, dataset, config).await.unwrap();
        let ordered = match limit {
            Some(limit) => ordered.limit(limit).unwrap(),
            None => ordered,
        };
        let ordering = ordered.ordering.clone();
        let path = output_path(&ordered, dataset);
        let plan = output_format(dataset.format)
            .sink_frame(ordered, &path)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        (plan, ordering)
    })
}

/// The physical plan of `formulation` over `dataset` under `config`, run into a draining sink.
fn drained_plan(
    formulation: &Formulation,
    dataset: &FixtureDataset,
    config: SessionConfig,
    limit: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    block_on(async {
        let (_, ordered) = planned(formulation, dataset, config).await.unwrap();
        let ordered = match limit {
            Some(limit) => ordered.limit(limit).unwrap(),
            None => ordered,
        };
        sink_plan(ordered).await.unwrap()
    })
}

/// Plans `formulation` over `dataset` under `config`, returning its session with the ordered frame.
async fn planned(
    formulation: &Formulation,
    dataset: &FixtureDataset,
    config: SessionConfig,
) -> Result<(SessionContext, OrderedFrame)> {
    let ctx = SessionContext::new_with_config(config);
    dataset.fixture.register(&ctx);
    let ordered = formulation.plan(&ctx, &dataset.dataset).await?;
    Ok((ctx, ordered))
}

/// The physical plan of `ordered` run into a draining sink.
async fn sink_plan(ordered: OrderedFrame) -> Result<Arc<dyn ExecutionPlan>> {
    sink::drain(ordered)?.create_physical_plan().await
}

/// Runs `formulation` through the collecting sink after applying `adjust` only to its frame.
fn collected_batches<Adjust>(
    formulation: &Formulation,
    dataset: FixtureDataset,
    adjust: Adjust,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>)
where
    Adjust: FnOnce(DataFrame) -> Result<DataFrame> + Send + 'static,
{
    let formulation = formulation.clone();
    pipeline::run(
        move |_| async move {
            let (ctx, ordered) = planned(&formulation, &dataset, hostile_config(8)).await?;
            let OrderedFrame {
                frame,
                ordering,
                layout,
            } = ordered;
            let ordered = OrderedFrame {
                frame: adjust(frame)?,
                ordering,
                layout,
            };
            let (frame, collected) = sink::collect(ordered)?;
            let plan = frame.create_physical_plan().await?;
            datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            Ok((plan, collected.take()))
        },
        PipelineOptions {
            threads: 2,
            ..Default::default()
        },
    )
    .unwrap()
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

/// Every one of the `n_scans` scans displays a predicate over the representation's locus column.
/// `EXPLAIN` is the public observation of a pushed filter, and each format names it differently.
/// That the predicate is the whole filter is shown by the results: with no filter operator
/// anywhere in the plan, only the scans could have narrowed the rows.
fn assert_filter_reaches_every_scan(
    plan: &Arc<dyn ExecutionPlan>,
    n_scans: usize,
    representation: LocusRepresentation,
) {
    let locus_column = LocusOrdering::locus()
        .expand(representation)
        .column_names()
        .into_iter()
        .next()
        .expect("a locus ordering has a stored field");
    let scans = nodes_of::<DataSourceExec>(plan);
    assert_eq!(scans.len(), n_scans, "{}", displayed(plan));
    for scan in &scans {
        let text = displayed(scan);
        let predicate = text
            .split_once("predicate=")
            .or_else(|| text.split_once("predicate:"))
            .map(|(_, predicate)| predicate);
        assert!(
            predicate.is_some_and(|predicate| predicate.contains(&locus_column)),
            "expected a filter on {locus_column} to reach the scan: {text}"
        );
    }
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
