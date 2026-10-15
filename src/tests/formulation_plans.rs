//! Shared datasets and plans for the suites of `formulation` and its plan builders.
//!
//! The suites plan a formulation, or one of `formulation`'s plan builders directly, over the
//! shared dataset fixtures, then observe the plan through a sink, as every action runs it: the
//! sink's ordering requirement is what holds a merge per input group beneath the final merge, so
//! a bare frame's physical plan is not the plan a run executes. See ADR 0014.

use crate::{
    format::OutputFormat,
    formulation::Formulation,
    locus::{Locus, LocusRepresentation, RowOrdering, StoredOrdering},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    sink,
    stored::dataset::Dataset,
    testing::fixture::{self, FixtureFormat, INPUT_TABLES, Row, block_on},
    tests::support::hostile_config,
    write::WriteTarget,
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::Result,
    physical_plan::ExecutionPlan,
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use futures::{FutureExt, future::BoxFuture};

use std::{num::NonZeroUsize, sync::Arc};

pub(crate) const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
pub(crate) const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

/// Something that builds an ordered frame over a dataset: a formulation, or a suite's handle on
/// one of `formulation`'s plan builders.
pub(crate) trait PlanBuilder: Clone + Send + Sync + 'static {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>>;
}

impl PlanBuilder for Formulation {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        Self::plan(self, ctx, dataset).boxed()
    }
}

pub(crate) struct FixtureDataset {
    pub(crate) fixture: &'static Arc<fixture::DatasetFixture>,
    pub(crate) dataset: Dataset,
    pub(crate) format: FixtureFormat,
    /// The names of the dataset's input tables, in name order, as the fixture inventory lists
    /// them rather than as discovery finds them.
    pub(crate) input_tables: &'static [&'static str],
}

impl FixtureDataset {
    /// This dataset restricted to its last `n` input tables. The last two of the mixed dataset are
    /// a multi-sample table and a single-sample one.
    pub(crate) fn restricted_to_last(self, n: usize) -> Self {
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
pub(crate) fn shared_datasets(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> [FixtureDataset; 2] {
    [
        dataset(format, representation),
        mixed_dataset(format, representation),
    ]
}

pub(crate) fn dataset(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> FixtureDataset {
    dataset_with_ordering(
        format,
        representation,
        Formulation::CombineAllelesUnion.required_ordering(),
    )
}

/// The mixed dataset fixture: multi-sample input tables of both kinds and a single-sample one.
pub(crate) fn mixed_dataset(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> FixtureDataset {
    discovered(
        fixture::mixed_dataset_fixture(format, representation),
        fixture::MIXED_INPUT_TABLES,
        format,
        Formulation::CombineAllelesUnion.required_ordering(),
    )
}

pub(crate) fn dataset_with_ordering(
    format: FixtureFormat,
    representation: LocusRepresentation,
    ordering: RowOrdering,
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
    ordering: RowOrdering,
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

/// Builds a plan under settings that would split an unpinned file scan.
/// The sorted table must keep one partition per sample even when the optimizer
/// is allowed to split files of any size.
pub(crate) fn physical_plan(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
) -> Arc<dyn ExecutionPlan> {
    drained_plan(builder, dataset, hostile_config(8), None)
}

pub(crate) fn physical_plan_with_target(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    drained_plan(builder, dataset, hostile_config(target_partitions), None)
}

/// The fixture format as an output format, so a plan writes what it read.
pub(crate) const fn output_format(format: FixtureFormat) -> OutputFormat {
    match format {
        FixtureFormat::Parquet => OutputFormat::PARQUET,
        FixtureFormat::Vortex => OutputFormat::VORTEX,
    }
}

/// The in-memory path a write of `ordered` over `dataset` goes to: a file named with the output
/// format's extension, or a directory for a frame written one file per partition.
pub(crate) fn output_path(ordered: &OrderedFrame, dataset: &FixtureDataset) -> String {
    let root = dataset.fixture.output_root();
    match ordered.layout {
        OutputLayout::SingleFile => format!(
            "{root}/combined.{}",
            output_format(dataset.format).extension()
        ),
        OutputLayout::FilePerPartition => format!("{root}/combined"),
    }
}

/// The physical plan `builder` builds over `dataset` under `config`, with a row limit of `limit`
/// if given, run into the fixture format's file sink at an in-memory path. Planned, not executed.
/// Returns the ordering carried by the frame so the sink requirement can be checked against it.
pub(crate) fn file_sink_plan(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
    config: SessionConfig,
    limit: Option<usize>,
) -> (Arc<dyn ExecutionPlan>, StoredOrdering) {
    block_on(async {
        let (_, ordered) = planned(builder, dataset, config).await.unwrap();
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

/// The physical plan `builder` builds over `dataset` under `config`, run into a draining sink.
pub(crate) fn drained_plan(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
    config: SessionConfig,
    limit: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    drained_plan_and_ordering(builder, dataset, config, limit).0
}

/// [`drained_plan`] with the ordering carried by the frame, so the sink requirement can be checked
/// against it.
pub(crate) fn drained_plan_and_ordering(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
    config: SessionConfig,
    limit: Option<usize>,
) -> (Arc<dyn ExecutionPlan>, StoredOrdering) {
    block_on(async {
        let (_, ordered) = planned(builder, dataset, config).await.unwrap();
        let ordered = match limit {
            Some(limit) => ordered.limit(limit).unwrap(),
            None => ordered,
        };
        let ordering = ordered.ordering.clone();
        (sink_plan(ordered).await.unwrap(), ordering)
    })
}

/// Plans with `builder` over `dataset` under `config`, returning its session with the ordered
/// frame.
pub(crate) async fn planned(
    builder: &impl PlanBuilder,
    dataset: &FixtureDataset,
    config: SessionConfig,
) -> Result<(SessionContext, OrderedFrame)> {
    let ctx = SessionContext::new_with_config(config);
    dataset.fixture.register(&ctx);
    let ordered = builder.plan(&ctx, &dataset.dataset).await?;
    Ok((ctx, ordered))
}

/// The physical plan of `ordered` run into a draining sink.
pub(crate) async fn sink_plan(ordered: OrderedFrame) -> Result<Arc<dyn ExecutionPlan>> {
    sink::drain(ordered)?.create_physical_plan().await
}

/// Runs `builder`'s plan through the collecting sink after applying `adjust` only to its frame.
pub(crate) fn collected_batches<Builder, Adjust>(
    builder: &Builder,
    dataset: FixtureDataset,
    adjust: Adjust,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>)
where
    Builder: PlanBuilder,
    Adjust: FnOnce(DataFrame) -> Result<DataFrame> + Send + 'static,
{
    let builder = builder.clone();
    pipeline::run(
        move |_| async move {
            let (ctx, ordered) = planned(&builder, &dataset, hostile_config(8)).await?;
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

/// The rows `builder` plans over the fixture of `format` and `representation`, collected through
/// the collecting sink in order.
pub(crate) fn collected_rows(
    builder: &impl PlanBuilder,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Vec<Row> {
    let (_, batches) = collected_batches(builder, dataset(format, representation), Ok);
    batches
        .iter()
        .flat_map(|batch| fixture::decode_rows(batch, representation))
        .collect()
}

/// The locus of each of `rows`, in order.
pub(crate) fn loci(rows: &[Row]) -> Vec<Locus> {
    rows.iter().map(|(locus, _, _)| *locus).collect()
}
