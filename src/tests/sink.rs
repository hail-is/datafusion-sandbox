//! The sinks an action runs its frame into, and the ordering requirement they put on the plan.

use crate::fixture::{self, DatasetFixture, FixtureFormat, SAMPLES, block_on};
use crate::{
    dataset::{Dataset, ScanShape},
    formulation::Formulation,
    locus::{LocusRepresentation, StoredOrdering},
    pipeline::{self, PipelineOptions},
    sink,
};

use datafusion::{
    arrow::array::{Array, UInt64Array},
    datasource::sink::DataSinkExec,
    physical_expr::expressions::Column,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
    prelude::{DataFrame, SessionContext},
};

use std::sync::Arc;

/// The number of rows every fixture dataset holds across its samples.
const FIXTURE_ROWS: usize = 32;

#[test]
fn the_drained_frame_returns_the_row_count() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let batches = pipeline::run(
        move |_| async move {
            let (_ctx, frame, ordering) = flat_read(fixture).await;
            sink::drain(frame, &ordering)?.collect().await
        },
        one_thread(),
    )
    .unwrap();

    let [batch] = batches.as_slice() else {
        panic!("expected one count batch, got {batches:?}");
    };
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count.value(0), u64::try_from(FIXTURE_ROWS).unwrap());
}

#[test]
fn the_collecting_sink_keeps_the_rows_in_the_required_order() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture_of(format);
        let (count, batches) = pipeline::run(
            move |_| async move {
                let (_ctx, frame, ordering) = flat_read(fixture).await;
                let (frame, collected) = sink::collect(frame, &ordering)?;
                let count = frame.collect().await?;
                Ok((count, collected.take()))
            },
            one_thread(),
        )
        .unwrap();

        assert_eq!(count.len(), 1, "{format:?}");
        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
            .collect();
        assert_eq!(rows.len(), FIXTURE_ROWS, "{format:?}");
        assert!(rows.is_sorted(), "{format:?}: {rows:?}");
    }
}

#[test]
fn taking_the_collected_batches_empties_the_sink() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let (first, second) = pipeline::run(
        move |_| async move {
            let (_ctx, frame, ordering) = flat_read(fixture).await;
            let (frame, collected) = sink::collect(frame, &ordering)?;
            frame.collect().await?;
            Ok((collected.take(), collected.take()))
        },
        one_thread(),
    )
    .unwrap();

    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(second.len(), 0, "{second:?}");
}

/// The sink's requirement is what turns an unordered union of ordered scans into a merge: no
/// sort appears above the frame, and the plan ends in the sink.
#[test]
fn the_sink_requires_the_ordering_and_the_optimizer_merges_to_meet_it() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture_of(format);
        let plan = block_on(async {
            let (_ctx, frame, ordering) = flat_read(fixture).await;
            sink::drain(frame, &ordering)
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap()
        });

        let sink_exec = plan
            .downcast_ref::<DataSinkExec>()
            .unwrap_or_else(|| panic!("{format:?}: expected the plan to end in a sink"));
        let requirement = sink_exec
            .sort_order()
            .as_ref()
            .unwrap_or_else(|| panic!("{format:?}: expected the sink to require an ordering"));
        let required: Vec<&str> = requirement
            .iter()
            .map(|sort| {
                sort.expr
                    .downcast_ref::<Column>()
                    .map_or("<not a column>", Column::name)
            })
            .collect();
        assert_eq!(required, ["contig", "position"], "{format:?}");

        let merges = nodes_of::<SortPreservingMergeExec>(&plan);
        assert_eq!(merges.len(), 1, "{format:?}:\n{}", displayed(&plan));
        assert_eq!(
            merges[0].children()[0]
                .output_partitioning()
                .partition_count(),
            SAMPLES.len(),
            "{format:?}:\n{}",
            displayed(&plan)
        );
        assert!(
            nodes_of::<SortExec>(&plan).is_empty(),
            "{format:?}:\n{}",
            displayed(&plan)
        );
    }
}

#[test]
fn each_sink_displays_its_own_name() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let (drained, collected) = block_on(async {
        let (_ctx, frame, ordering) = flat_read(fixture).await;
        let drained = sink::drain(frame.clone(), &ordering)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let (frame, _) = sink::collect(frame, &ordering).unwrap();
        let collected = frame.create_physical_plan().await.unwrap();
        (drained, collected)
    });

    assert!(
        displayed(&drained).starts_with("DataSinkExec: sink=DrainingSink"),
        "{}",
        displayed(&drained)
    );
    assert!(
        displayed(&collected).starts_with("DataSinkExec: sink=CollectingSink"),
        "{}",
        displayed(&collected)
    );
}

/// The contig-position fixture in `format`, built outside any runtime as the fixture requires.
fn fixture_of(format: FixtureFormat) -> &'static Arc<DatasetFixture> {
    fixture::dataset_fixture(format, LocusRepresentation::ContigPosition)
}

/// The flat union of the fixture's samples with no sort above it, and the reference combiner's
/// ordering over that dataset.
async fn flat_read(fixture: &DatasetFixture) -> (SessionContext, DataFrame, StoredOrdering) {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    fixture.register(&ctx);
    let layout = Formulation::CombineRefsUnion.required_layout();
    let dataset = Dataset::discover(
        &ctx,
        fixture.table_path().clone(),
        fixture.input_format(),
        layout.clone(),
        None,
    )
    .await
    .unwrap();
    let ordering = dataset.query_ordering(&layout.locus_ordering).unwrap();
    let frame = dataset.read(&ctx, &ScanShape::Flat).await.unwrap();
    (ctx, frame, ordering)
}

fn one_thread() -> PipelineOptions {
    PipelineOptions {
        threads: 1,
        ..Default::default()
    }
}

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
