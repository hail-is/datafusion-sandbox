//! Sinks: the operator every action runs its frame into, and the ordering it requires.
//!
//! Every action ends in a sink whose required input ordering is the formulation's ordering. The
//! requirement is what keeps a merge per sample group, or per locus interval, beneath the sink; a
//! logical sort in the same place destroys it. The file sink is the format's; the two `DataSink`s
//! here stand in for it when the action collects rows or only analyzes the plan. See
//! [ADR 0014](../docs/adr/0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md).
//!
//! `DataFusion`'s own `DataSinkExec` merges its input to one partition before writing. The
//! [`PartitionedSinkExec`] here instead runs a sink plan per input partition, so a formulation
//! whose partitions are the locus intervals writes one file per interval from one plan. See
//! [ADR 0015](../docs/adr/0015-write-one-file-per-partition-through-a-partitioned-sink.md).

use crate::locus::StoredOrdering;

use datafusion::{
    arrow::{
        compute::SortOptions,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    common::{DFSchema, tree_node::TreeNodeRecursion},
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
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
        ExecutionPlanProperties, InputDistributionRequirements, Partitioning, PlanProperties,
        ReplaceChildrenOptions, SendableRecordBatchStream,
        execution_plan::{EvaluationType, SchedulingType},
        metrics::MetricsSet,
    },
    prelude::{DataFrame, Expr},
};
use futures_util::TryStreamExt;

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
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

/// The frame that runs `df` into a sink keeping every batch it receives, in arrival order.
///
/// Executing the frame yields the row count; the batches are taken from the returned sink
/// afterwards.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn collect(
    df: DataFrame,
    ordering: &StoredOrdering,
) -> Result<(DataFrame, Arc<CollectingSink>)> {
    let sink = Arc::new(CollectingSink::new(Arc::clone(df.schema().inner())));
    let target = Arc::new(DataSinkTarget(sink.clone()));
    let frame = run_into(df, "collect", Some(ordering), target)?;
    Ok((frame, sink))
}

/// The frame that runs `df` into a sink counting its rows and dropping them.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn drain(df: DataFrame, ordering: &StoredOrdering) -> Result<DataFrame> {
    let sink = Arc::new(DrainingSink {
        schema: Arc::clone(df.schema().inner()),
    });
    run_into(df, "drain", Some(ordering), Arc::new(DataSinkTarget(sink)))
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
struct DataSinkTarget(Arc<dyn DataSink>);

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
/// A partition sink is a single-partition plan whose only child is
/// [`PartitionedSinkExec::input_partition`] for its index, so a format builds it exactly as it
/// builds a single file's sink and the count schema is the sink's. The partition sinks are built
/// once, when the target is planned, for the partitions the input has then; the optimizer swaps its
/// final input beneath every one of them through `replace_children`. That call accepts an input
/// with any partition count, because distribution enforcement probes the sink with a hypothetical
/// coalesced child to compare pipeline behavior; executing a sink whose input's partition count no
/// longer matches its sinks fails with an internal error rather than writing the wrong files.
#[derive(Debug)]
pub struct PartitionedSinkExec {
    input: Arc<dyn ExecutionPlan>,
    partition_sinks: Vec<Arc<dyn ExecutionPlan>>,
    ordering: Option<LexRequirement>,
    cache: Arc<PlanProperties>,
}

impl PartitionedSinkExec {
    /// The single-partition plan yielding partition `index` of `input`, for a partition sink to
    /// read as its only input partition.
    #[must_use]
    pub fn input_partition(input: Arc<dyn ExecutionPlan>, index: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(InputPartitionExec::new(input, index))
    }

    /// The sink over `input` whose `partition_sinks[i]` writes input partition `i`, requiring
    /// `ordering` of every partition.
    ///
    /// # Errors
    ///
    /// Returns an error if a sink does not read its partition through
    /// [`Self::input_partition`], yield one partition, or produce the count schema.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_sinks: Vec<Arc<dyn ExecutionPlan>>,
        ordering: Option<LexRequirement>,
    ) -> Result<Self> {
        let count = input.output_partitioning().partition_count();
        let count_schema = count_schema();
        for (index, sink) in partition_sinks.iter().enumerate() {
            let reads_its_partition = match sink.children().as_slice() {
                [child] => child
                    .downcast_ref::<InputPartitionExec>()
                    .is_some_and(|child| child.index == index && Arc::ptr_eq(&child.input, &input)),
                _ => false,
            };
            if !reads_its_partition {
                return Err(DataFusionError::Internal(format!(
                    "partition sink {index} does not read input partition {index}"
                )));
            }
            if sink.output_partitioning().partition_count() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "partition sink {index} yields {} partitions instead of one",
                    sink.output_partitioning().partition_count()
                )));
            }
            if sink.schema().fields() != count_schema.fields() {
                return Err(DataFusionError::Internal(format!(
                    "partition sink {index} yields {} instead of the count schema",
                    sink.schema()
                )));
            }
        }
        let cache = PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            Partitioning::UnknownPartitioning(count),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .with_evaluation_type(EvaluationType::Eager);
        Ok(Self {
            input,
            partition_sinks,
            ordering,
            cache: Arc::new(cache),
        })
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
                Arc::clone(sink).replace_children(
                    vec![Self::input_partition(Arc::clone(&input), index)],
                    options,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self::try_new(
            input,
            partition_sinks,
            self.ordering.clone(),
        )?))
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
