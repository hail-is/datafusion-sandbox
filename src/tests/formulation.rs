//! Plan shape tests for both combiners.
//!
//! What matters about these plans is not what they return but how they get
//! there: a sort-preserving merge over one partition per sample, or one per
//! input group with a merge per group beneath it, and no re-sort. A regression
//! from merging to re-sorting is invisible in the results and costs an order of
//! magnitude in time, so it is asserted structurally here.
//!
//! Every plan here is observed through a sink, as every action runs it: the
//! sink's ordering requirement is what holds a merge per input group beneath
//! the final merge, so a bare frame's physical plan is not the plan a run
//! executes. See ADR 0014.
//!
//! The tests here build unfiltered plans of the single-file formulations.
//! `filtered_plans` holds the same formulations under a caller's contig or
//! locus-interval filter, `grouped_merge` holds what is particular to merging
//! by input group, and `interval_merge` holds the interval-merge formulation,
//! whose plan has one scan per sample per locus interval and whose write ends
//! in the partitioned sink rather than the file sink.

mod filtered_plans;
mod grouped_merge;
mod interval_merge;

use crate::fixture;

use crate::{
    format::OutputFormat,
    formulation::Formulation,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    run_metrics::FormulationRecord,
    sink,
    stored::dataset::{Dataset, InputTable},
    tests::{
        plan_shape::PlanShape,
        support::{grouped_merge, hostile_config, interval_merge},
    },
    write::WriteTarget,
};
use datafusion::{
    arrow::record_batch::RecordBatch,
    common::DataFusionError,
    datasource::sink::DataSinkExec,
    error::Result,
    physical_plan::ExecutionPlan,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use std::{num::NonZeroUsize, sync::Arc};

use fixture::{FixtureFormat, INPUT_TABLES, SAMPLES, block_on};

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

/// Each formulation describes its recorded settings: its name, and the group count or split
/// points only the formulation that takes one has.
#[test]
fn each_formulation_describes_its_recorded_settings() {
    let record =
        |name: &str, groups: Option<usize>, split_points: Option<&str>| FormulationRecord {
            name: name.to_string(),
            groups,
            split_points: split_points.map(ToString::to_string),
        };

    for (formulation, expected) in [
        (
            Formulation::CombineAllelesUnion,
            record("union", None, None),
        ),
        (Formulation::CombineRefsUnion, record("union", None, None)),
        (grouped_merge(3), record("grouped-merge", Some(3), None)),
        (
            interval_merge("1:5,2:1"),
            record("interval-merge", None, Some("1:5,2:1")),
        ),
    ] {
        assert_eq!(
            FormulationRecord::from(&formulation),
            expected,
            "{formulation:?}"
        );
    }
}

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
fn the_allele_combiner_rejects_a_dataset_with_a_multi_sample_input_table() {
    for format in FORMATS {
        let dataset = mixed_dataset(format, LocusRepresentation::ContigPosition);
        let ctx = SessionContext::new();
        dataset.fixture.register(&ctx);

        let error = block_on(Formulation::CombineAllelesUnion.plan(&ctx, &dataset.dataset))
            .expect_err("the allele combiner reads only single-sample input tables");

        assert!(matches!(error, DataFusionError::Plan(_)));
        let message = error.to_string();
        assert!(
            message.contains("multi-sample"),
            "unexpected error: {message}"
        );
        assert!(message.contains("'g0'"), "unexpected error: {message}");
    }
}

/// Over every shared dataset, each formulation merges each input table as one ordered partition,
/// grouped-merge forms its groups from input tables, and the draining sink requires the
/// formulation's ordering.
#[test]
fn formulations_keep_their_plan_shape_under_a_hostile_session() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let (plan, ordering) =
                        drained_plan_and_ordering(formulation, &dataset, hostile_config(8), None);
                    let shape = PlanShape::of(&plan);
                    shape.assert_merge_tree(&expected_groups(
                        formulation,
                        dataset.input_tables.len(),
                    ));
                    shape.assert_ends_in_sink_requiring(&ordering);
                }
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
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let (plan, ordering) =
                        file_sink_plan(formulation, &dataset, hostile_config(8), None);
                    let shape = PlanShape::of(&plan);
                    assert!(plan.is::<DataSinkExec>(), "{shape}");
                    shape.assert_merge_tree(&expected_groups(
                        formulation,
                        dataset.input_tables.len(),
                    ));
                    shape.assert_ends_in_sink_requiring(&ordering);
                }
            }
        }
    }
}

#[test]
fn target_partitions_do_not_introduce_sorts_into_either_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let single_target = physical_plan_with_target(formulation, &dataset, 1);
                    let eight_targets = physical_plan_with_target(formulation, &dataset, 8);
                    PlanShape::of(&single_target).assert_no_sorts();
                    PlanShape::of(&eight_targets).assert_no_sorts();
                }
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
        PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
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
        PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
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
        PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
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
        PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
    }
}

/// Restricting a shared dataset to its last two input tables merges two inputs. Over the mixed
/// dataset those are one of each kind, the multi-sample one still one input.
#[test]
fn restricting_the_input_tables_changes_input_count_for_every_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                let formulations = formulations_over(&dataset);
                let dataset = dataset.restricted_to_last(2);
                for formulation in &formulations {
                    let plan = physical_plan(formulation, &dataset);
                    PlanShape::of(&plan).assert_merge_tree(&expected_groups(formulation, 2));
                }
            }
        }
    }
}

/// The input groups a formulation's plan should merge over `n_input_tables`, as their sizes,
/// counted in input tables, in name order. Written out rather than computed, so the test cannot agree with a
/// wrong split. Interval-merge merges every input table once per locus interval;
/// `interval_merge` spells that out.
fn expected_groups(formulation: &Formulation, n_input_tables: usize) -> Vec<usize> {
    match (formulation, n_input_tables) {
        (Formulation::CombineRefsGroupedMerge { groups }, 4) if *groups == GROUPS => vec![2, 2],
        (Formulation::CombineRefsGroupedMerge { groups }, 3) if *groups == GROUPS => vec![2, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, 2) if *groups == GROUPS => vec![1, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, n) => {
            panic!("no expected groups for {groups} groups over {n} input tables")
        }
        (Formulation::CombineRefsIntervalMerge { split_points }, n) => {
            panic!("no expected groups for split points {split_points} over {n} input tables")
        }
        (Formulation::CombineRefsUnion | Formulation::CombineAllelesUnion, n) => vec![n],
    }
}

/// The reference combiner's formulations among the shared list.
fn reference_formulations() -> [Formulation; 2] {
    let [union, grouped_merge, _] = formulations();
    [union, grouped_merge]
}

/// The shared formulations that plan over `dataset`: every one, or only the reference combiner's
/// if the dataset holds a multi-sample input table, which the allele combiner rejects.
fn formulations_over(dataset: &FixtureDataset) -> Vec<Formulation> {
    if dataset
        .dataset
        .input_tables()
        .iter()
        .any(InputTable::is_multi_sample)
    {
        reference_formulations().to_vec()
    } else {
        formulations().to_vec()
    }
}

struct FixtureDataset {
    fixture: &'static Arc<fixture::DatasetFixture>,
    dataset: Dataset,
    format: FixtureFormat,
    /// The names of the dataset's input tables, in name order, as the fixture inventory lists
    /// them rather than as discovery finds them.
    input_tables: &'static [&'static str],
}

impl FixtureDataset {
    /// This dataset restricted to its last `n` input tables. The last two of the mixed dataset are
    /// a multi-sample table and a single-sample one.
    fn restricted_to_last(self, n: usize) -> Self {
        let input_tables = &self.input_tables[self.input_tables.len().checked_sub(n).unwrap()..];
        let requested = input_tables
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        Self {
            dataset: self.dataset.restrict_to(&requested).unwrap(),
            input_tables,
            ..self
        }
    }
}

/// The shared dataset fixtures the loops run over: one of single-sample input tables only, and
/// the mixed one.
fn shared_datasets(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> [FixtureDataset; 2] {
    [
        dataset(format, representation),
        mixed_dataset(format, representation),
    ]
}

fn dataset(format: FixtureFormat, representation: LocusRepresentation) -> FixtureDataset {
    dataset_with_ordering(
        format,
        representation,
        Formulation::CombineAllelesUnion.required_ordering(),
    )
}

/// The mixed dataset fixture: multi-sample input tables of both kinds and a single-sample one.
fn mixed_dataset(format: FixtureFormat, representation: LocusRepresentation) -> FixtureDataset {
    discovered(
        fixture::mixed_dataset_fixture(format, representation),
        fixture::MIXED_INPUT_TABLES,
        format,
        Formulation::CombineAllelesUnion.required_ordering(),
    )
}

fn dataset_with_ordering(
    format: FixtureFormat,
    representation: LocusRepresentation,
    ordering: LocusOrdering,
) -> FixtureDataset {
    discovered(
        fixture::dataset_fixture(format, representation),
        INPUT_TABLES,
        format,
        ordering,
    )
}

fn discovered(
    fixture: &'static Arc<fixture::DatasetFixture>,
    input_tables: &'static [&'static str],
    format: FixtureFormat,
    ordering: LocusOrdering,
) -> FixtureDataset {
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
        input_tables,
    }
}

/// Builds a formulation under settings that would split an unpinned file scan.
/// The sorted table must keep one partition per sample even when the optimizer
/// is allowed to split files of any size.
fn physical_plan(formulation: &Formulation, dataset: &FixtureDataset) -> Arc<dyn ExecutionPlan> {
    drained_plan(formulation, dataset, hostile_config(8), None)
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
    let root = dataset.fixture.output_root();
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
        let plan = WriteTarget {
            output_path: path,
            output_format: output_format(dataset.format),
        }
        .sink_frame(ordered)
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
    drained_plan_and_ordering(formulation, dataset, config, limit).0
}

/// [`drained_plan`] with the ordering carried by the frame, so the sink requirement can be checked
/// against it.
fn drained_plan_and_ordering(
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
        (sink_plan(ordered).await.unwrap(), ordering)
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
        PipelineOptions::new(NonZeroUsize::new(2).unwrap()),
    )
    .unwrap()
}
