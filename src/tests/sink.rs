//! The sinks an action runs its frame into, and the ordering requirement they put on the plan.

use crate::fixture::{self, DatasetFixture, FixtureFormat, SAMPLES, block_on};
use crate::{
    dataset::Dataset,
    formulation::Formulation,
    locus::LocusRepresentation,
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    sink::{self, CollectingSink, PartitionedSinkExec, SinkTarget},
};

use datafusion::{
    arrow::array::{Array, UInt64Array},
    catalog::Session,
    datasource::sink::{DataSink, DataSinkExec},
    error::{DataFusionError, Result},
    physical_expr::{LexRequirement, expressions::Column},
    physical_plan::{
        Distribution, ExecutionPlan, ExecutionPlanProperties,
        coalesce_partitions::CoalescePartitionsExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
    prelude::SessionContext,
};

use std::sync::{Arc, Mutex, PoisonError};

/// The number of rows every fixture dataset holds across its samples.
const FIXTURE_ROWS: usize = 32;

#[test]
fn the_drained_frame_returns_the_row_count() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let batches = pipeline::run(
        move |_| async move {
            let (_ctx, frame) = flat_read(fixture).await;
            sink::drain(frame)?.collect().await
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
                let (_ctx, frame) = flat_read(fixture).await;
                let (frame, collected) = sink::collect(frame)?;
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
            let (_ctx, frame) = flat_read(fixture).await;
            let (frame, collected) = sink::collect(frame)?;
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
            let (_ctx, frame) = flat_read(fixture).await;
            sink::drain(frame)
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
        let (_ctx, frame) = flat_read(fixture).await;
        let collected_frame = OrderedFrame {
            frame: frame.frame.clone(),
            ordering: frame.ordering.clone(),
            layout: frame.layout,
        };
        let drained = sink::drain(frame)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let (frame, _) = sink::collect(collected_frame).unwrap();
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

/// The partitioned sink runs input partition `i` into partition sink `i` and nothing else: over
/// the flat union of four ordered sample scans, each collecting sink receives one sample's eight
/// rows in locus order, and the frame yields one count batch per partition.
#[test]
fn the_partitioned_sink_writes_each_partition_to_its_own_sink() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture_of(format);
        let target = Arc::new(CollectingPartitions::default());
        let counts = {
            let target = Arc::clone(&target);
            pipeline::run(
                move |_| async move {
                    let (_ctx, frame) = flat_read(fixture).await;
                    sink::run_into(frame.frame, "partitions", Some(&frame.ordering), target)?
                        .collect()
                        .await
                },
                one_thread(),
            )
            .unwrap()
        };

        let counts: Vec<u64> = counts
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(counts, vec![8; SAMPLES.len()], "{format:?}");
        let sinks = target.sinks();
        assert_eq!(sinks.len(), SAMPLES.len(), "{format:?}");
        let mut samples_seen = Vec::new();
        for sink in &sinks {
            let batches = sink.take();
            let rows: Vec<_> = batches
                .iter()
                .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
                .collect();
            assert_eq!(rows.len(), 8, "{format:?}");
            assert!(rows.is_sorted(), "{format:?}: {rows:?}");
            let mut samples: Vec<String> = batches
                .iter()
                .flat_map(|batch| fixture::string_column(batch, "s"))
                .collect();
            samples.dedup();
            assert_eq!(samples.len(), 1, "{format:?}: {samples:?}");
            samples_seen.extend(samples);
        }
        assert_eq!(samples_seen, SAMPLES, "{format:?}");
    }
}

/// The partitioned sink keeps its input's partitions, requires the ordering of each, and asks for
/// neither a distribution nor more partitions, so the optimizer adds no merge or coalesce
/// beneath it. Its partition sinks read the optimized input, and it displays the sink it holds.
#[test]
fn the_partitioned_sink_requires_the_ordering_per_partition_and_keeps_the_partitions() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let plan = block_on(async {
        let (_ctx, frame) = flat_read(fixture).await;
        sink::run_into(
            frame.frame,
            "partitions",
            Some(&frame.ordering),
            Arc::new(CollectingPartitions::default()),
        )
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
    });

    let sink_exec = plan
        .downcast_ref::<PartitionedSinkExec>()
        .unwrap_or_else(|| panic!("expected the partitioned sink:\n{}", displayed(&plan)));
    assert_eq!(plan.output_partitioning().partition_count(), SAMPLES.len());
    let required: Vec<&str> = sink_exec
        .ordering()
        .expect("a requirement")
        .iter()
        .map(|sort| {
            sort.expr
                .downcast_ref::<Column>()
                .map_or("<not a column>", Column::name)
        })
        .collect();
    assert_eq!(required, ["contig", "position"]);
    assert_eq!(plan.required_input_ordering().len(), 1);
    assert!(plan.required_input_ordering()[0].is_some());
    assert_eq!(plan.benefits_from_input_partitioning(), [false]);
    assert_eq!(plan.maintains_input_order(), [true]);
    assert!(matches!(
        plan.input_distribution_requirements()
            .into_per_child()
            .as_slice(),
        [Distribution::UnspecifiedDistribution]
    ));
    assert!(
        nodes_of::<SortPreservingMergeExec>(&plan).is_empty()
            && nodes_of::<CoalescePartitionsExec>(&plan).is_empty()
            && nodes_of::<SortExec>(&plan).is_empty(),
        "{}",
        displayed(&plan)
    );
    for (index, partition_sink) in sink_exec.partition_sinks().iter().enumerate() {
        let input_partition = &partition_sink.children()[0];
        assert!(
            Arc::ptr_eq(input_partition.children()[0], plan.children()[0]),
            "partition sink {index} does not read the sink's input"
        );
        assert_eq!(input_partition.output_partitioning().partition_count(), 1);
    }
    assert!(
        displayed(&plan).starts_with("PartitionedSinkExec: partitions=4, sink=CollectingSink"),
        "{}",
        displayed(&plan)
    );
}

/// The partitioned sink refuses sinks that read the input some other way than through its
/// input-partition plan for their own index. Fewer sinks than input partitions are accepted when
/// built, because the optimizer probes the sink with hypothetical children, and refused when
/// executed.
#[test]
fn the_partitioned_sink_rejects_sinks_that_do_not_match_its_partitions() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let input = block_on(async {
        let (_ctx, frame) = flat_read(fixture).await;
        frame.frame.create_physical_plan().await.unwrap()
    });
    let schema = input.schema();
    let sink = |input: Arc<dyn ExecutionPlan>| -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSinkExec::new(
            input,
            Arc::new(CollectingSink::new(Arc::clone(&schema))),
            None,
        ))
    };

    let too_few: Arc<dyn ExecutionPlan> = Arc::new(
        PartitionedSinkExec::try_new(
            Arc::clone(&input),
            vec![sink(PartitionedSinkExec::input_partition(
                Arc::clone(&input),
                0,
            ))],
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        too_few.output_partitioning().partition_count(),
        SAMPLES.len()
    );
    let executed = pipeline::run(
        move |ctx| async move { datafusion::physical_plan::collect(too_few, ctx.task_ctx()).await },
        one_thread(),
    );
    assert!(
        matches!(executed, Err(DataFusionError::Internal(_))),
        "{executed:?}"
    );

    let mut sinks: Vec<Arc<dyn ExecutionPlan>> = (0..SAMPLES.len())
        .map(|index| {
            sink(PartitionedSinkExec::input_partition(
                Arc::clone(&input),
                index,
            ))
        })
        .collect();
    sinks.swap(0, 1);
    let out_of_order = PartitionedSinkExec::try_new(Arc::clone(&input), sinks, None);
    assert!(
        matches!(out_of_order, Err(DataFusionError::Internal(_))),
        "{out_of_order:?}"
    );

    let whole_input: Vec<Arc<dyn ExecutionPlan>> = (0..SAMPLES.len())
        .map(|_| sink(Arc::clone(&input)))
        .collect();
    let not_a_partition = PartitionedSinkExec::try_new(Arc::clone(&input), whole_input, None);
    assert!(
        matches!(not_a_partition, Err(DataFusionError::Internal(_))),
        "{not_a_partition:?}"
    );

    let matching: Vec<Arc<dyn ExecutionPlan>> = (0..SAMPLES.len())
        .map(|index| {
            sink(PartitionedSinkExec::input_partition(
                Arc::clone(&input),
                index,
            ))
        })
        .collect();
    let accepted = PartitionedSinkExec::try_new(input, matching, None).unwrap();
    assert_eq!(accepted.partition_sinks().len(), SAMPLES.len());
}

/// A target running each partition of its input into its own collecting sink, kept for the test
/// to read after the frame has run.
#[derive(Debug, Default)]
struct CollectingPartitions {
    sinks: Mutex<Vec<Arc<CollectingSink>>>,
}

impl CollectingPartitions {
    fn sinks(&self) -> Vec<Arc<CollectingSink>> {
        self.sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl SinkTarget for CollectingPartitions {
    async fn plan(
        &self,
        _: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let count = input.output_partitioning().partition_count();
        let sinks: Vec<Arc<CollectingSink>> = (0..count)
            .map(|_| Arc::new(CollectingSink::new(input.schema())))
            .collect();
        let partition_sinks = sinks
            .iter()
            .enumerate()
            .map(|(index, sink)| {
                let data_sink: Arc<dyn DataSink> = sink.clone();
                let partition_sink: Arc<dyn ExecutionPlan> = Arc::new(DataSinkExec::new(
                    PartitionedSinkExec::input_partition(Arc::clone(&input), index),
                    data_sink,
                    ordering.clone(),
                ));
                partition_sink
            })
            .collect();
        *self.sinks.lock().unwrap_or_else(PoisonError::into_inner) = sinks;
        Ok(Arc::new(PartitionedSinkExec::try_new(
            input,
            partition_sinks,
            ordering,
        )?))
    }
}

/// The contig-position fixture in `format`, built outside any runtime as the fixture requires.
fn fixture_of(format: FixtureFormat) -> &'static Arc<DatasetFixture> {
    fixture::dataset_fixture(format, LocusRepresentation::ContigPosition)
}

/// The flat union of the fixture's samples with no sort above it, and the reference combiner's
/// ordering over that dataset.
async fn flat_read(fixture: &DatasetFixture) -> (SessionContext, OrderedFrame) {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    fixture.register(&ctx);
    let required_ordering = Formulation::CombineRefsUnion.required_ordering();
    let dataset = Dataset::discover(
        &ctx,
        fixture.table_path().clone(),
        fixture.input_format(),
        required_ordering.clone(),
        None,
    )
    .await
    .unwrap();
    let frame = OrderedFrame {
        frame: dataset.read(&ctx).await.unwrap(),
        ordering: dataset.query_ordering(&required_ordering).unwrap(),
        layout: OutputLayout::SingleFile,
    };
    (ctx, frame)
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
