//! The sinks an action runs its frame into, and the ordering requirement they put on the plan.

use super::plan_shape::PlanShape;
use crate::fixture::{self, DatasetFixture, FixtureFormat, SAMPLES, block_on};
use crate::{
    formulation::Formulation,
    generated::{make_range_table, make_unbounded_table},
    locus::LocusRepresentation,
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    run_metrics,
    sink::{
        self, CollectingSink, DataSinkTarget, DrainingSink, PartitionedSinkExec, ProbedSink,
        SinkTarget,
    },
    stored::dataset::Dataset,
    tests::support::{rows_of_operator, u64_values},
    throughput_probe::{ProbeSettings, StopReason},
};

use datafusion::{
    arrow::{
        array::{Array, UInt64Array},
        record_batch::RecordBatch,
    },
    catalog::Session,
    datasource::sink::DataSinkExec,
    error::{DataFusionError, Result},
    physical_expr::LexRequirement,
    physical_plan::{
        ChildrenPropertiesMode, Distribution, ExecutionPlan, ExecutionPlanProperties,
        ReplaceChildrenOptions,
        coalesce_partitions::CoalescePartitionsExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
    prelude::{DataFrame, SessionContext, col, lit},
};

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

/// The number of rows every fixture dataset holds across its samples.
const FIXTURE_ROWS: usize = 32;

/// Executing a sink frame through the seam yields the rows the sink wrote and the plan that wrote
/// them: its root is the sink, and an operator beneath it holds the baseline metrics it
/// accumulated while running. A generated table and a collecting sink keep the test off every
/// store.
#[test]
fn executing_a_sink_frame_retains_the_plan_it_ran() {
    let (executed, collected) = pipeline::run(
        |ctx| async move {
            let frame = make_range_table(&ctx, 1000, 128)?
                .select(vec![(col("idx") * lit(2)).alias("doubled")])?;
            let sink = Arc::new(CollectingSink::new(Arc::clone(frame.schema().inner())));
            let target = Arc::new(DataSinkTarget::new(sink.clone()));
            let executed =
                sink::execute_and_retain(sink::run_into(frame, "collect", None, target)?).await?;
            Ok((executed, sink.take()))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_eq!(executed.rows_written, 1000);
    assert!(executed.execute_ns > 0);
    assert_eq!(
        collected.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1000
    );
    let shape = PlanShape::of(&executed.plan);
    assert!(executed.plan.is::<DataSinkExec>(), "{shape}");
    let projections = shape.nodes_of::<ProjectionExec>();
    let [projection] = projections.as_slice() else {
        panic!("expected one projection beneath the sink:\n{shape}");
    };
    let metrics = projection
        .metrics()
        .unwrap_or_else(|| panic!("the projection reports no metrics:\n{shape}"));
    assert_eq!(metrics.output_rows(), Some(1000), "{shape}");
    assert!(
        metrics.elapsed_compute().is_some_and(|nanos| nanos > 0),
        "{shape}"
    );
}

#[test]
fn the_drained_frame_returns_the_row_count() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let batches = pipeline::run(
        move |_| async move {
            let (_ctx, frame) = flat_read(fixture).await;
            sink::drain(frame)?.collect().await
        },
        PipelineOptions::single_threaded(),
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
            PipelineOptions::single_threaded(),
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
        PipelineOptions::single_threaded(),
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
        let (plan, ordering) = block_on(async {
            let (_ctx, frame) = flat_read(fixture).await;
            let ordering = frame.ordering.clone();
            let plan = sink::drain(frame)
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            (plan, ordering)
        });

        let shape = PlanShape::of(&plan);
        assert!(plan.is::<DataSinkExec>(), "{format:?}:\n{shape}");
        shape.assert_ends_in_sink_requiring(&ordering);
        shape.assert_merge_tree(&[SAMPLES.len()]);
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

    let drained = PlanShape::of(&drained);
    assert!(
        drained
            .to_string()
            .starts_with("DataSinkExec: sink=DrainingSink"),
        "{drained}"
    );
    let collected = PlanShape::of(&collected);
    assert!(
        collected
            .to_string()
            .starts_with("DataSinkExec: sink=CollectingSink"),
        "{collected}"
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
                PipelineOptions::single_threaded(),
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
    let (plan, ordering) = block_on(async {
        let (_ctx, frame) = flat_read(fixture).await;
        let ordering = frame.ordering.clone();
        let plan = sink::run_into(
            frame.frame,
            "partitions",
            Some(&frame.ordering),
            Arc::new(CollectingPartitions::default()),
        )
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
        (plan, ordering)
    });

    let shape = PlanShape::of(&plan);
    shape.assert_ends_in_sink_requiring(&ordering);
    let sink_exec = plan
        .downcast_ref::<PartitionedSinkExec>()
        .unwrap_or_else(|| panic!("expected the partitioned sink:\n{shape}"));
    assert_eq!(plan.output_partitioning().partition_count(), SAMPLES.len());
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
        shape.nodes_of::<SortPreservingMergeExec>().is_empty()
            && shape.nodes_of::<CoalescePartitionsExec>().is_empty()
            && shape.nodes_of::<SortExec>().is_empty(),
        "{shape}"
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
        shape
            .to_string()
            .starts_with("PartitionedSinkExec: partitions=4, sink=CollectingSink"),
        "{shape}"
    );
}

/// The partitioned sink refuses a target whose plan is no sink over its partition: rows handed
/// back unwritten do not carry the count schema, and counts from two sinks at once do not carry
/// one partition.
#[test]
fn the_partitioned_sink_rejects_a_target_that_does_not_yield_counts() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    block_on(async {
        let (ctx, frame) = flat_read(fixture).await;
        let input = frame.frame.create_physical_plan().await.unwrap();
        let state = ctx.state();

        let unwritten: Arc<dyn SinkTarget> = Arc::new(PlanAsGiven);
        let not_the_count_schema =
            PartitionedSinkExec::plan(&state, Arc::clone(&input), None, &|_, _| {
                Arc::clone(&unwritten)
            })
            .await;
        assert_internal_error(not_the_count_schema, "instead of the count schema");

        let two_sinks: Arc<dyn SinkTarget> = Arc::new(PlanInstead(counts_of_two_sinks(&input)));
        let more_than_one_partition =
            PartitionedSinkExec::plan(&state, Arc::clone(&input), None, &|_, _| {
                Arc::clone(&two_sinks)
            })
            .await;
        assert_internal_error(more_than_one_partition, "partitions instead of one");
    });
}

/// Two sinks over `input` unioned: the count schema, in two partitions rather than one.
fn counts_of_two_sinks(input: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let one_sink = || -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSinkExec::new(
            Arc::new(CoalescePartitionsExec::new(Arc::clone(input))),
            Arc::new(CollectingSink::new(Arc::clone(&schema))),
            None,
        ))
    };
    UnionExec::try_new(vec![one_sink(), one_sink()]).unwrap()
}

/// Asserts `result` is the internal error whose message contains `expected`, so that a case
/// cannot pass by tripping a different check.
fn assert_internal_error<T: fmt::Debug>(result: Result<T>, expected: &str) {
    match result {
        Err(DataFusionError::Internal(message)) => assert!(
            message.contains(expected),
            "expected an error mentioning {expected:?}, got {message:?}"
        ),
        other => panic!("expected an internal error mentioning {expected:?}, got {other:?}"),
    }
}

/// The partitioned sink refuses to execute once its input's partition count no longer matches the
/// sinks it was planned over. Distribution enforcement probes the sink with a coalesced child, so
/// the swap itself must succeed; only the execution fails. See ADR 0015.
#[test]
fn the_partitioned_sink_refuses_to_execute_when_its_input_partitions_change() {
    let fixture = fixture_of(FixtureFormat::Vortex);
    let swapped = block_on(async {
        let (ctx, frame) = flat_read(fixture).await;
        let input = frame.frame.create_physical_plan().await.unwrap();
        let planned = CollectingPartitions::default()
            .plan(&ctx.state(), Arc::clone(&input), None)
            .await
            .unwrap();
        assert_eq!(
            planned.output_partitioning().partition_count(),
            SAMPLES.len()
        );
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
        planned
            .replace_children(
                vec![coalesced],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap()
    });
    assert_eq!(swapped.output_partitioning().partition_count(), 1);

    let executed = pipeline::run(
        move |ctx| async move { datafusion::physical_plan::collect(swapped, ctx.task_ctx()).await },
        PipelineOptions::single_threaded(),
    );
    assert_internal_error(executed, "was planned over");
}

/// A target that is no sink at all: it hands back the single partition it was given, rows and
/// schema unchanged.
#[derive(Debug)]
struct PlanAsGiven;

#[async_trait::async_trait]
impl SinkTarget for PlanAsGiven {
    async fn plan(
        &self,
        _: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(input)
    }
}

/// A target ignoring the partition it was given and handing back a plan of its own.
#[derive(Debug)]
struct PlanInstead(Arc<dyn ExecutionPlan>);

#[async_trait::async_trait]
impl SinkTarget for PlanInstead {
    async fn plan(
        &self,
        _: &dyn Session,
        _: Arc<dyn ExecutionPlan>,
        _: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self.0))
    }
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
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let count = input.output_partitioning().partition_count();
        let sinks: Vec<Arc<CollectingSink>> = (0..count)
            .map(|_| Arc::new(CollectingSink::new(input.schema())))
            .collect();
        let partition = |index: usize, _: usize| -> Arc<dyn SinkTarget> {
            Arc::new(DataSinkTarget::new(sinks[index].clone()))
        };
        let exec = PartitionedSinkExec::plan(state, input, ordering, &partition).await?;
        *self.sinks.lock().unwrap_or_else(PoisonError::into_inner) = sinks;
        Ok(Arc::new(exec))
    }
}

/// Probing a plan that never finishes stops it at the maximum duration, capped, with no
/// partition end. The samples start near the start of execution, take the time forward, and
/// their rows only increase; the plan dropped at the stop still reports its metrics, where the
/// operator feeding the sink holds the rows the probe says the sink received.
#[test]
fn probing_a_plan_that_never_finishes_stops_it_capped() {
    let max_duration = Duration::from_millis(300);
    let (probed, filter_rows) = probe_generated(max_duration, |ctx| make_unbounded_table(ctx, 64));

    assert_eq!(probed.decision.stop_reason, StopReason::Capped);
    assert_eq!(probed.first_partition_end_ns, None);
    let samples = &probed.samples;
    assert!(samples.len() >= 3, "{samples:?}");
    assert!(samples[0].elapsed_ns < samples[1].elapsed_ns, "{samples:?}");
    let last = samples.last().unwrap();
    assert!(
        u128::from(last.elapsed_ns) >= max_duration.as_nanos(),
        "{samples:?}"
    );
    for pair in samples.windows(2) {
        assert!(pair[0].elapsed_ns < pair[1].elapsed_ns, "{pair:?}");
        assert!(pair[0].rows <= pair[1].rows, "{pair:?}");
    }
    assert!(last.rows > samples[0].rows, "{samples:?}");
    assert_eq!(probed.decision.window_end_ns, last.elapsed_ns);
    assert!(probed.rows_received >= last.rows);
    assert!(probed.execute_ns >= last.elapsed_ns);
    assert_eq!(filter_rows, probed.rows_received);
}

/// Probing a plan that finishes before its cap completes at the sample that first shows its
/// partition finished, which closes the window, having received every row.
#[test]
fn probing_a_plan_that_finishes_completes_at_its_partition_end() {
    let (probed, filter_rows) = probe_generated(Duration::from_secs(60), |ctx| {
        make_range_table(ctx, 10_000, 64)
    });

    assert_eq!(probed.decision.stop_reason, StopReason::Completed);
    let last = probed.samples.last().unwrap();
    assert_eq!(probed.first_partition_end_ns, Some(last.elapsed_ns));
    assert_eq!(probed.decision.window_end_ns, last.elapsed_ns);
    assert_eq!(last.rows, 10_000);
    assert_eq!(probed.rows_received, 10_000);
    assert_eq!(filter_rows, 10_000);
}

/// Probing with a zero poll period, which no sampler can keep, is refused with an error that
/// names the setting, before the plan executes.
#[test]
fn probing_with_a_zero_poll_period_is_refused() {
    let settings = ProbeSettings {
        poll_period: Duration::ZERO,
        ..ProbeSettings::default()
    };

    let error = try_probe_generated(settings, |ctx| make_range_table(ctx, 100, 64)).unwrap_err();

    let message = error.to_string();
    assert!(message.contains("poll period"), "{message}");
}

/// Probes a drain of the generated table `build` makes, sampling every 10 ms, and returns what
/// [`try_probe_generated`] returns.
fn probe_generated(
    max_duration: Duration,
    build: impl FnOnce(&SessionContext) -> Result<DataFrame> + Send + 'static,
) -> (ProbedSink, u64) {
    let settings = ProbeSettings {
        poll_period: Duration::from_millis(10),
        max_duration,
        ..ProbeSettings::default()
    };
    try_probe_generated(settings, build).unwrap()
}

/// Probes a drain of the generated table `build` makes with `settings`, filtered so that the
/// operator feeding the sink counts its rows. Returns the probe and the `output_rows` of the
/// filter, over its partitions, in the run metrics of the plan it stopped.
fn try_probe_generated(
    settings: ProbeSettings,
    build: impl FnOnce(&SessionContext) -> Result<DataFrame> + Send + 'static,
) -> Result<(ProbedSink, u64)> {
    pipeline::run(
        move |ctx| async move {
            let frame = build(&ctx)?.filter(col("idx").gt_eq(lit(0)))?;
            let sink = Arc::new(DrainingSink::new(Arc::clone(frame.schema().inner())));
            let target = Arc::new(DataSinkTarget::new(sink));
            let probed =
                sink::probe(sink::run_into(frame, "drain", None, target)?, &settings).await?;
            let metrics = run_metrics::run_metrics_batch("probe", &probed.plan)?.batch;
            let output_rows = u64_values(&metrics, "output_rows");
            let filter_rows = rows_of_operator(&metrics, "FilterExec")
                .into_iter()
                .map(|row| output_rows[row].unwrap())
                .sum();
            Ok((probed, filter_rows))
        },
        PipelineOptions::single_threaded(),
    )
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
