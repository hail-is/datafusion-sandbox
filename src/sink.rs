//! Sinks: the operator every action runs its frame into, and the ordering it requires.
//!
//! Every action ends in a sink whose required input ordering is the formulation's ordering. The
//! requirement is what keeps a merge per sample group, or per locus interval, beneath the sink; a
//! logical sort in the same place destroys it. The file sink is the write module's; the two
//! `DataSink`s here stand in for it when the action collects rows or only analyzes the plan. See
//! [ADR 0014](../docs/adr/0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md).
//!
//! `DataFusion`'s own `DataSinkExec` merges its input to one partition before writing. The
//! [`PartitionedSinkExec`] here instead runs a sink plan per input partition, so a formulation
//! whose partitions are the locus intervals writes one file per interval from one plan. See
//! [ADR 0015](../docs/adr/0015-write-one-file-per-partition-through-a-partitioned-sink.md).
//!
//! The [`SinkTarget`] implementations here are the format-free collecting and draining sinks. A
//! write's file-sink targets live in [`crate::write`].
//!
//! [`execute_and_retain`] is how a sink frame runs: it builds the physical plan, times its
//! execution to completion, and hands the plan back with the rows written, so every operator's
//! metrics are readable from it afterwards. A frame-level collect would drop the plan with the
//! batches.
//!
//! [`probe`] runs a sink frame the same way for a throughput probe, but only until the probe's
//! decision stops it, taking progress samples on the IO runtime as it goes, and hands the plan
//! back as it stood at the stop.

use crate::{
    locus::StoredOrdering,
    ordered_frame::OrderedFrame,
    pipeline,
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample},
};

use datafusion::{
    arrow::{
        array::{Array, UInt64Array},
        compute::SortOptions,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    common::{DFSchema, runtime::JoinSet, tree_node::TreeNodeRecursion},
    datasource::{
        DefaultTableSource,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result},
    execution::TaskContext,
    logical_expr::{LogicalPlanBuilder, SortExpr, TableType, dml::InsertOp},
    physical_expr::{
        EquivalenceProperties, LexRequirement, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
    },
    physical_plan::{
        self, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
        ExecutionPlanProperties, InputDistributionRequirements, Partitioning, PlanProperties,
        ReplaceChildrenOptions, SendableRecordBatchStream,
        execution_plan::{EvaluationType, SchedulingType},
        metrics::{MetricValue, MetricsSet},
    },
    prelude::{DataFrame, Expr},
};
use futures_util::{StreamExt, TryStreamExt, future};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, MissedTickBehavior},
};

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

/// What an insert plans to once its input and the ordering requirement are known.
#[async_trait::async_trait]
pub trait SinkTarget: fmt::Debug + Send + Sync {
    /// The execution plan that writes `input` into this target, requiring `ordering` of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the target's plan cannot be built.
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// The frame that runs `df` into `target` when executed, requiring `ordering` of the rows.
///
/// `None` places no requirement. The planner reaches the target through the frame's table
/// source; nothing is registered on the session.
///
/// # Errors
///
/// Returns an error if the insert plan cannot be built.
pub fn run_into(
    df: DataFrame,
    name: &str,
    ordering: Option<&StoredOrdering>,
    target: Arc<dyn SinkTarget>,
) -> Result<DataFrame> {
    let provider = Arc::new(SinkProvider {
        schema: Arc::clone(df.schema().inner()),
        ordering: ordering.map(StoredOrdering::sort_expressions),
        target,
    });
    let (state, plan) = df.into_parts();
    let plan = LogicalPlanBuilder::insert_into(
        plan,
        name,
        Arc::new(DefaultTableSource::new(provider)),
        InsertOp::Append,
    )?
    .build()?;
    Ok(DataFrame::new(state, plan))
}

/// What executing a sink frame yields: the rows the sink wrote, the physical plan that wrote them,
/// and the nanoseconds spent executing that plan. Every operator's metrics are readable from the
/// plan.
#[derive(Debug)]
pub struct ExecutedSink {
    pub rows_written: u64,
    pub plan: Arc<dyn ExecutionPlan>,
    pub execute_ns: u64,
}

/// Executes `frame`, a sink frame, to completion and keeps hold of the physical plan it ran.
///
/// This is the one path a sink frame executes on. The plan is built and run the way a frame-level
/// collect builds and runs it, on the frame's own session, and then kept rather than dropped. Its
/// execution duration starts after physical-plan construction and ends when collection completes.
///
/// # Errors
///
/// Returns an error if the plan cannot be built or executed, or if the sink yields anything other
/// than count batches.
pub async fn execute_and_retain(frame: DataFrame) -> Result<ExecutedSink> {
    let task_ctx = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let executing = Instant::now();
    let batches = physical_plan::collect(Arc::clone(&plan), task_ctx).await?;
    let execute_ns = elapsed_ns(executing);
    let rows_written = rows_written(&batches)?;
    Ok(ExecutedSink {
        rows_written,
        plan,
        execute_ns,
    })
}

/// What probing a sink frame yields: the plan it stopped, when and why it stopped, and its
/// progress samples.
///
/// Every operator's metrics are readable from the plan, as they stood at the stop.
#[derive(Debug)]
pub struct ProbedSink {
    /// The rows the operator feeding the sink had emitted when the probe stopped.
    pub rows_received: u64,
    pub plan: Arc<dyn ExecutionPlan>,
    /// Nanoseconds from the start of execution to the stop.
    pub execute_ns: u64,
    /// Every progress sample taken, in order.
    pub samples: Vec<ProgressSample>,
    /// The elapsed nanoseconds of the first sample that showed a finished partition of the
    /// operator feeding the sink, if one did before the stop.
    pub first_partition_end_ns: Option<u64>,
    pub decision: Decision,
}

/// Executes `frame`, a sink frame, until [`throughput_probe::decide`] stops it, and keeps hold of
/// the physical plan it ran.
///
/// The plan is built as [`execute_and_retain`] builds it and executed as a stream. A sampler on
/// the session's IO runtime reads the metrics of the operator feeding the sink every poll period,
/// starting as execution starts, and after each reading the probe decides whether to stop. The
/// stream is polled only once the first reading is in, so the samples start at the start of
/// execution. When the stream ends before a decision, the sampler takes one last reading at once.
/// Stopping drops the stream, which aborts the plan's tasks.
///
/// # Errors
///
/// Returns an error if the poll period is zero, the session has no IO runtime, the plan cannot be
/// built or executed, the sink has no single input, or that input reports no output rows or ends
/// without a finished partition.
pub async fn probe(frame: DataFrame, settings: &ProbeSettings) -> Result<ProbedSink> {
    if settings.poll_period.is_zero() {
        return Err(DataFusionError::Configuration(
            "a throughput probe's poll period must be positive".to_string(),
        ));
    }
    let task_ctx = Arc::new(frame.task_ctx());
    let io_runtime = pipeline::io_runtime(task_ctx.session_config())?;
    let plan = frame.create_physical_plan().await?;
    let feeding = match plan.children().as_slice() {
        [feeding] => Arc::clone(feeding),
        children => {
            return Err(DataFusionError::Internal(format!(
                "a probed sink needs one input, but {} has {}",
                plan.name(),
                children.len()
            )));
        }
    };

    let (readings_sender, mut readings) = mpsc::unbounded_channel();
    let (finish, finishing) = oneshot::channel::<()>();
    let executing = Instant::now();
    let mut unpolled = Some(physical_plan::execute_stream(Arc::clone(&plan), task_ctx)?);
    let mut sampling = JoinSet::new();
    sampling.spawn_on(
        sample(
            Arc::clone(&feeding),
            executing,
            settings.poll_period,
            readings_sender,
            finishing,
        ),
        &io_runtime,
    );

    let mut polling = None;
    let mut finish = Some(finish);
    let mut samples = Vec::new();
    let mut first_partition_end_ns = None;
    let decision = loop {
        tokio::select! {
            reading = readings.recv() => {
                let Some(Reading { elapsed_ns, rows, finished }) = reading else {
                    return Err(DataFusionError::Internal(format!(
                        "the plan ended, but {}, the operator feeding its sink, reported no finished partition",
                        feeding.name()
                    )));
                };
                let rows = rows.ok_or_else(|| DataFusionError::Internal(format!(
                    "{}, the operator feeding the sink, reports no output rows to sample",
                    feeding.name()
                )))?;
                samples.push(ProgressSample { elapsed_ns, rows });
                if finished && first_partition_end_ns.is_none() {
                    first_partition_end_ns = Some(elapsed_ns);
                }
                if let Some(decision) =
                    throughput_probe::decide(settings, &samples, first_partition_end_ns)
                {
                    break decision;
                }
                if let Some(stream) = unpolled.take() {
                    polling = Some(stream);
                }
            }
            batch = next_batch(&mut polling) => {
                if let Some(batch) = batch {
                    batch?;
                } else {
                    polling = None;
                    // Dropping the sender resolves the sampler's receiver: its last reading.
                    drop(finish.take());
                }
            }
        }
    };
    drop(polling.or(unpolled));
    let execute_ns = elapsed_ns(executing);
    drop(sampling);
    let rows_received = feeding
        .metrics()
        .and_then(|metrics| metrics.output_rows())
        .map_or(0, to_u64);
    Ok(ProbedSink {
        rows_received,
        plan,
        execute_ns,
        samples,
        first_partition_end_ns,
        decision,
    })
}

/// One reading of the operator feeding a probed sink.
struct Reading {
    elapsed_ns: u64,
    /// `None` if the operator reports no output rows.
    rows: Option<u64>,
    /// Whether any partition of the operator has an end timestamp.
    finished: bool,
}

/// Reads `feeding` every `poll_period` from now on, and once more at once when `finishing`
/// resolves, as it does when its sender is dropped, sending each reading with its time since
/// `executing`. Stops after that last reading, or once the readings are no longer received.
async fn sample(
    feeding: Arc<dyn ExecutionPlan>,
    executing: Instant,
    poll_period: Duration,
    readings: mpsc::UnboundedSender<Reading>,
    mut finishing: oneshot::Receiver<()>,
) {
    let mut ticks = time::interval(poll_period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let last = tokio::select! {
            _ = ticks.tick() => false,
            _ = &mut finishing => true,
        };
        let metrics = feeding.metrics();
        let elapsed_ns = elapsed_ns(executing);
        let reading = Reading {
            elapsed_ns,
            rows: metrics
                .as_ref()
                .and_then(MetricsSet::output_rows)
                .map(to_u64),
            finished: metrics.iter().flat_map(MetricsSet::iter).any(|metric| {
                matches!(metric.value(), MetricValue::EndTimestamp(end) if end.value().is_some())
            }),
        };
        if readings.send(reading).is_err() || last {
            return;
        }
    }
}

/// The next batch of `stream`, or never if there is none to poll.
async fn next_batch(stream: &mut Option<SendableRecordBatchStream>) -> Option<Result<RecordBatch>> {
    match stream {
        Some(stream) => stream.next().await,
        None => future::pending().await,
    }
}

/// Wall-clock nanoseconds since `since`, saturating at `u64::MAX`.
fn elapsed_ns(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// A count as a probe reports it. Saturates rather than failing on a platform whose `usize` is
/// wider than 64 bits.
fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The frame that runs `df` into a sink keeping every batch it receives, in arrival order.
///
/// Executing the frame yields the row count; the batches are taken from the returned sink
/// afterwards.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn collect(ordered: OrderedFrame) -> Result<(DataFrame, Arc<CollectingSink>)> {
    let sink = Arc::new(CollectingSink::new(Arc::clone(
        ordered.frame.schema().inner(),
    )));
    let target = Arc::new(DataSinkTarget::new(sink.clone()));
    let frame = run_into(ordered.frame, "collect", Some(&ordered.ordering), target)?;
    Ok((frame, sink))
}

/// The frame that runs `df` into a sink counting its rows and dropping them.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn drain(ordered: OrderedFrame) -> Result<DataFrame> {
    let sink = Arc::new(DrainingSink::new(Arc::clone(
        ordered.frame.schema().inner(),
    )));
    run_into(
        ordered.frame,
        "drain",
        Some(&ordered.ordering),
        Arc::new(DataSinkTarget::new(sink)),
    )
}

/// A sink that keeps the batches written to it.
#[derive(Debug)]
pub struct CollectingSink {
    schema: SchemaRef,
    batches: Mutex<Vec<RecordBatch>>,
}

impl CollectingSink {
    /// An empty sink accepting batches of `schema`.
    #[must_use]
    pub const fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Mutex::new(Vec::new()),
        }
    }

    /// Takes the batches written so far, leaving the sink empty.
    pub fn take(&self) -> Vec<RecordBatch> {
        std::mem::take(&mut *self.batches.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl DisplayAs for CollectingSink {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CollectingSink")
    }
}

#[async_trait::async_trait]
impl DataSink for CollectingSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> Result<u64> {
        count_rows(data, |batch| {
            self.batches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(batch);
        })
        .await
    }
}

/// A sink that counts the rows written to it and keeps nothing.
#[derive(Debug)]
pub struct DrainingSink {
    schema: SchemaRef,
}

impl DrainingSink {
    /// A sink accepting batches of `schema`.
    #[must_use]
    pub const fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DisplayAs for DrainingSink {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DrainingSink")
    }
}

#[async_trait::async_trait]
impl DataSink for DrainingSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> Result<u64> {
        count_rows(data, drop).await
    }
}

/// Consumes `data`, handing each batch to `on_batch`, and returns the number of rows seen.
async fn count_rows(
    mut data: SendableRecordBatchStream,
    mut on_batch: impl FnMut(RecordBatch) + Send,
) -> Result<u64> {
    let mut rows = 0_u64;
    while let Some(batch) = data.try_next().await? {
        rows = rows.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        on_batch(batch);
    }
    Ok(rows)
}

/// A target that is a `DataSink` behind `DataFusion`'s own `DataSinkExec`.
#[derive(Debug)]
pub(crate) struct DataSinkTarget(Arc<dyn DataSink>);

impl DataSinkTarget {
    /// The target writing into `sink`.
    pub(crate) const fn new(sink: Arc<dyn DataSink>) -> Self {
        Self(sink)
    }
}

#[async_trait::async_trait]
impl SinkTarget for DataSinkTarget {
    async fn plan(
        &self,
        _: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::clone(&self.0),
            ordering,
        )))
    }
}

/// A sink over every partition of its input: input partition `i` runs into the `i`th partition
/// sink, and the exec yields that sink's count batch as its own partition `i`.
///
/// `DataSinkExec` requires a single input partition and executes only partition 0, so one plan
/// through it writes one file. This exec requires the ordering of every input partition, places no
/// distribution requirement, and does not benefit from partitioning, so the optimizer neither
/// coalesces nor repartitions beneath it. See
/// [ADR 0015](../docs/adr/0015-write-one-file-per-partition-through-a-partitioned-sink.md).
///
/// The exec builds the single-partition input each partition sink reads, so a sink is never paired
/// with the wrong partition. The partition sinks are built once, when the target is planned, for
/// the partitions the input has then; the optimizer swaps its final input beneath every one of them
/// through `replace_children`. That call accepts an input with any partition count, because
/// distribution enforcement probes the sink with a hypothetical coalesced child to compare pipeline
/// behavior; executing a sink whose input's partition count no longer matches its sinks fails with
/// an internal error rather than writing the wrong files.
#[derive(Debug)]
pub struct PartitionedSinkExec {
    input: Arc<dyn ExecutionPlan>,
    partition_sinks: Vec<Arc<dyn ExecutionPlan>>,
    ordering: Option<LexRequirement>,
    cache: Arc<PlanProperties>,
}

impl PartitionedSinkExec {
    /// The sink over `input` running each of its partitions into the sink its own target plans,
    /// requiring `ordering` of every partition.
    ///
    /// `target(index, count)` supplies the target for partition `index` of `count`. The exec reads
    /// the partition count off `input` and builds the single-partition plan each target writes, so
    /// there is no protocol for a caller to get wrong.
    ///
    /// `ordering` reaches every partition sink as well as this exec, because a format turns it into
    /// file metadata there, such as Parquet's sorting columns. It is not enforced there: the
    /// partition sinks are not this exec's children, so the optimizer never walks them. What the
    /// optimizer satisfies is the requirement this exec reports for its own input.
    ///
    /// # Errors
    ///
    /// Returns an error if a target's plan cannot be built, or if it yields anything other than one
    /// partition of the count schema.
    pub async fn plan(
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
        target: &(dyn Fn(usize, usize) -> Arc<dyn SinkTarget> + Send + Sync),
    ) -> Result<Self> {
        let count = input.output_partitioning().partition_count();
        let count_schema = count_schema();
        let mut partition_sinks = Vec::with_capacity(count);
        for index in 0..count {
            let partition = input_partition(Arc::clone(&input), index);
            let sink = target(index, count)
                .plan(state, partition, ordering.clone())
                .await?;
            if sink.output_partitioning().partition_count() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "the target for partition {index} yields {} partitions instead of one",
                    sink.output_partitioning().partition_count()
                )));
            }
            if sink.schema().fields() != count_schema.fields() {
                return Err(DataFusionError::Internal(format!(
                    "the target for partition {index} yields {} instead of the count schema",
                    sink.schema()
                )));
            }
            partition_sinks.push(sink);
        }
        Ok(Self {
            cache: Arc::new(Self::properties(&input)),
            input,
            partition_sinks,
            ordering,
        })
    }

    /// The properties of a sink over `input`: the count schema, and one output partition per input
    /// partition as `input` reports them now.
    fn properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(count_schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .with_evaluation_type(EvaluationType::Eager)
    }

    /// The ordering required of every input partition.
    #[must_use]
    pub const fn ordering(&self) -> Option<&LexRequirement> {
        self.ordering.as_ref()
    }

    /// The plans writing each input partition, in partition order.
    #[must_use]
    pub fn partition_sinks(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.partition_sinks
    }
}

impl DisplayAs for PartitionedSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "PartitionedSinkExec: partitions={}",
            self.partition_sinks.len()
        )?;
        if let Some(sink) = self
            .partition_sinks
            .first()
            .and_then(|sink| sink.downcast_ref::<DataSinkExec>())
        {
            formatter.write_str(", sink=")?;
            sink.sink().fmt_as(t, formatter)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for PartitionedSinkExec {
    fn name(&self) -> &'static str {
        "PartitionedSinkExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.ordering.clone().map(Into::into)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = match children.as_slice() {
            [input] => Arc::clone(input),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "PartitionedSinkExec takes one child, got {}",
                    children.len()
                )));
            }
        };
        let partition_sinks = self
            .partition_sinks
            .iter()
            .enumerate()
            .map(|(index, sink)| {
                Arc::clone(sink)
                    .replace_children(vec![input_partition(Arc::clone(&input), index)], options)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            cache: Arc::new(Self::properties(&input)),
            input,
            partition_sinks,
            ordering: self.ordering.clone(),
        }))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != self.partition_sinks.len() {
            return Err(DataFusionError::Internal(format!(
                "the partitioned sink was planned over {} partitions, but its input has \
                 {input_partitions}; see ADR 0015",
                self.partition_sinks.len()
            )));
        }
        let sink = self.partition_sinks.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "PartitionedSinkExec has {} partitions; partition {partition} was requested",
                self.partition_sinks.len()
            ))
        })?;
        sink.execute(0, context)
    }

    /// The metrics of every partition sink together.
    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = MetricsSet::new();
        for set in self
            .partition_sinks
            .iter()
            .filter_map(|sink| sink.metrics())
        {
            for metric in set.iter() {
                metrics.push(Arc::clone(metric));
            }
        }
        Some(metrics)
    }
}

/// The schema of a sink's output: one `count` of the rows it wrote.
fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// The rows written, summed over the count batches a sink plan yields: one from a single-file
/// sink, one per partition from a partitioned sink.
///
/// # Errors
///
/// Returns an error if `batches` is empty or holds anything other than the count schema.
fn rows_written(batches: &[RecordBatch]) -> Result<u64> {
    let malformed = || {
        DataFusionError::Internal(format!(
            "expected batches of one non-null count: UInt64 column from the sink, got {batches:?}"
        ))
    };
    if batches.is_empty() {
        return Err(malformed());
    }
    let mut total = 0_u64;
    for batch in batches {
        if batch.num_columns() != 1 {
            return Err(malformed());
        }
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .filter(|counts| counts.null_count() == 0)
            .ok_or_else(malformed)?;
        for count in counts.values() {
            total = total.checked_add(*count).ok_or_else(malformed)?;
        }
    }
    Ok(total)
}

/// The single-partition plan yielding partition `index` of `input`, for a partition sink to read as
/// its only input partition.
fn input_partition(input: Arc<dyn ExecutionPlan>, index: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(InputPartitionExec::new(input, index))
}

/// Partition `index` of `input` as a plan's only partition, for a single-partition sink to read.
#[derive(Debug)]
struct InputPartitionExec {
    input: Arc<dyn ExecutionPlan>,
    index: usize,
    cache: Arc<PlanProperties>,
}

impl InputPartitionExec {
    fn new(input: Arc<dyn ExecutionPlan>, index: usize) -> Self {
        let cache = PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            index,
            cache: Arc::new(cache),
        }
    }
}

impl DisplayAs for InputPartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "InputPartitionExec: partition={}", self.index)
    }
}

impl ExecutionPlan for InputPartitionExec {
    fn name(&self) -> &'static str {
        "InputPartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [input] => Ok(Arc::new(Self::new(Arc::clone(input), self.index))),
            _ => Err(DataFusionError::Internal(format!(
                "InputPartitionExec takes one child, got {}",
                children.len()
            ))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "InputPartitionExec has one partition; partition {partition} was requested"
            )));
        }
        self.input.execute(self.index, context)
    }
}

/// The table an insert writes to: a target and the ordering its input must arrive in.
#[derive(Debug)]
struct SinkProvider {
    schema: SchemaRef,
    ordering: Option<Vec<SortExpr>>,
    target: Arc<dyn SinkTarget>,
}

#[async_trait::async_trait]
impl TableProvider for SinkProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        _: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "a sink only accepts rows; it cannot be scanned".to_string(),
        ))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let requirement = match &self.ordering {
            Some(ordering) => {
                let input_schema = DFSchema::try_from(input.schema())?;
                let sort_exprs = ordering
                    .iter()
                    .map(|sort| {
                        let expr = state.create_physical_expr(sort.expr.clone(), &input_schema)?;
                        Ok(PhysicalSortExpr::new(
                            expr,
                            SortOptions::new(!sort.asc, sort.nulls_first),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                LexRequirement::new(sort_exprs.into_iter().map(Into::into))
            }
            None => None,
        };
        self.target.plan(state, input, requirement).await
    }
}
