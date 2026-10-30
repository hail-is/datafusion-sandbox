//! Ordering assertions and established orders: plan nodes that hold a key order, and fail planning
//! when the plan could meet it only by a sort the backend inserted.
//!
//! An **ordering assertion** states that rows already arrive in a key order at its point in the
//! plan. An **established order** is a sort a plan builder wrote, which the plan takes as stating
//! its key order from that point on. Both require their input in their order, over one partition
//! or within each, so the optimizer builds a merge tree beneath them rather than a sort: the same
//! requirement a sink placed until ADR 0022 moved it to an assertion at the root.
//!
//! [`hold`] places an assertion at the root of a frame and wraps every sort in it in an
//! established order. The check runs in `check_invariants` on the final physical plan, so a failed
//! assertion is an error raised before execution: [`OrderingAssertionFailed`], found beneath
//! whatever context the planner wraps it in. The check walks down from the assertion along the
//! edges on which nodes keep their input's order, through every input of a union or a merge. It
//! fails on a sort, accepts an established order and the sort beneath it, and stops at another
//! assertion, which checks its own subtree. It fails closed on any node with inputs that keeps
//! none of their orders. A scan is a leaf, so a scan that reorders whole files by their statistics
//! is not a sort.
//!
//! With [`verify_order`] on a session, every assertion and established order also checks at run
//! time that its rows arrive in its order, failing execution at the first that does not. It
//! catches an order the optimizer believed in but the rows lack, which no planning check can; see
//! [issue #325](https://github.com/hail-is/datafusion-sandbox/issues/325).

use crate::row_ordering::SortColumn;

use datafusion::{
    arrow::{
        array::ArrayRef,
        compute::SortOptions,
        row::{OwnedRow, RowConverter, SortField},
    },
    catalog::Session,
    common::{
        DFSchemaRef, DataFusionError, Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    execution::{
        SendableRecordBatchStream, TaskContext, context::QueryPlanner,
        session_state::SessionStateBuilder,
    },
    logical_expr::{
        Expr, Extension, LogicalPlan, Sort, SortExpr, UserDefinedLogicalNode,
        UserDefinedLogicalNodeCore, physical_planning_context::PhysicalPlanningContext,
    },
    physical_expr::{LexRequirement, OrderingRequirements, PhysicalSortRequirement},
    physical_plan::{
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
        InputDistributionRequirements, PhysicalExpr, PlanProperties, ReplaceChildrenOptions,
        execution_plan::InvariantLevel,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        sorts::sort_preserving_merge::SortPreservingMergeExec,
        sorts::{
            partial_sort::PartialSortExec, partitioned_topk::PartitionedTopKExec, sort::SortExec,
        },
        stream::RecordBatchStreamAdapter,
        union::UnionExec,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::{DataFrame, SessionConfig},
};
use futures_util::{StreamExt, stream};

use std::{cmp::Ordering, collections::HashSet, error::Error, fmt, sync::Arc};

/// Where an assertion holds its order: across one partition, which merges its input, or within
/// each of its input's partitions, which it keeps.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub enum AssertionScope {
    /// One partition in the order, as a single-file sink or a collect reads it.
    OnePartition,
    /// Every partition in the order, kept as the input partitions it, as a write of one file per
    /// partition reads it.
    EachPartition,
}

/// `frame` held in `ordering` by an ordering assertion at its root over `scope`, with every sort
/// in it wrapped in an established order.
///
/// The frame's session state gains a query planner that plans both nodes; it replaces the
/// session's own, which in this crate is always `DataFusion`'s default. Hold a frame once, as it
/// comes from its plan builder: holding it again wraps its sorts twice.
///
/// # Errors
///
/// Returns a planning error if `ordering` is empty.
pub fn hold(frame: DataFrame, ordering: &[SortColumn], scope: AssertionScope) -> Result<DataFrame> {
    if ordering.is_empty() {
        return Err(DataFusionError::Plan(
            "an ordering assertion needs at least one key".to_string(),
        ));
    }
    let (state, plan) = frame.into_parts();
    let established = plan
        .transform_up(|node| {
            let LogicalPlan::Sort(Sort { expr, .. }) = &node else {
                return Ok(Transformed::no(node));
            };
            let ordering = expr.clone();
            Ok(Transformed::yes(held(
                node,
                ordering,
                Kind::Established,
                AssertionScope::OnePartition,
            )))
        })?
        .data;
    let plan = held(
        established,
        ordering.iter().map(SortColumn::sort_expression).collect(),
        Kind::Asserted,
        scope,
    );
    let state = SessionStateBuilder::new_from_existing(state)
        .with_query_planner(Arc::new(HoldingQueryPlanner))
        .build();
    Ok(DataFrame::new(state, plan))
}

/// `config` with the run-time order check on: every assertion and established order planned on a
/// session of it checks the order of the rows it passes on, and fails execution at the first row
/// out of order.
#[must_use]
pub fn verify_order(config: SessionConfig) -> SessionConfig {
    config.with_extension(Arc::new(VerifyOrder))
}

/// The marker [`verify_order`] leaves in a session's configuration.
#[derive(Debug)]
struct VerifyOrder;

/// The error a failed assertion plans to: the plan could meet the assertion's order only by a sort
/// the backend inserted, or holds it through a node that keeps no input's order.
///
/// The planner wraps it in context; find it beneath with [`OrderingAssertionFailed::find`].
#[derive(Debug)]
pub struct OrderingAssertionFailed {
    message: String,
}

impl OrderingAssertionFailed {
    /// The failed assertion `error` holds, beneath any context the planner wrapped it in.
    #[must_use]
    pub fn find(error: &DataFusionError) -> Option<&Self> {
        match error.find_root() {
            DataFusionError::External(external) => external.downcast_ref(),
            _ => None,
        }
    }
}

impl fmt::Display for OrderingAssertionFailed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for OrderingAssertionFailed {}

/// What a held node is: an assertion, which checks the plan beneath it, or an established order,
/// which accepts the sort beneath it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
enum Kind {
    Asserted,
    Established,
}

/// `input` held in `ordering` by a node of `kind` over `scope`.
fn held(
    input: LogicalPlan,
    ordering: Vec<SortExpr>,
    kind: Kind,
    scope: AssertionScope,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(HeldOrder {
            input: Arc::new(input),
            ordering,
            kind,
            scope,
        }),
    })
}

/// The logical node of an assertion or an established order.
#[derive(Debug, Eq, Hash, PartialEq, PartialOrd)]
struct HeldOrder {
    input: Arc<LogicalPlan>,
    ordering: Vec<SortExpr>,
    kind: Kind,
    scope: AssertionScope,
}

impl HeldOrder {
    const fn label(&self) -> &'static str {
        match self.kind {
            Kind::Asserted => "OrderingAssertion",
            Kind::Established => "EstablishedOrder",
        }
    }
}

impl UserDefinedLogicalNodeCore for HeldOrder {
    fn name(&self) -> &str {
        self.label()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.ordering.iter().map(|sort| sort.expr.clone()).collect()
    }

    /// A filter keeps row order, so every filter may move beneath the node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys = self
            .ordering
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        write!(formatter, "{}: {}", self.label(), keys.join(", "))?;
        if self.scope == AssertionScope::EachPartition {
            formatter.write_str(", each partition")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return Err(DataFusionError::Internal(format!(
                "{} takes one input",
                self.label()
            )));
        };
        let ordering = self
            .ordering
            .iter()
            .zip(exprs)
            .map(|(sort, expr)| SortExpr::new(expr, sort.asc, sort.nulls_first))
            .collect();
        Ok(Self {
            input: Arc::new(input),
            ordering,
            kind: self.kind,
            scope: self.scope,
        })
    }

    /// Projections may prune columns beneath the node, keeping those its keys read.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let schema = self.input.schema();
        let mut needed = output_columns.to_vec();
        for sort in &self.ordering {
            for column in sort.expr.column_refs() {
                needed.push(schema.index_of_column(column).ok()?);
            }
        }
        needed.sort_unstable();
        needed.dedup();
        Some(vec![needed])
    }

    /// The node passes on every row it receives, in order, so a limit above it may move beneath.
    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// `DataFusion`'s default query planner, plus the planner of held orders.
#[derive(Debug)]
struct HoldingQueryPlanner;

#[async_trait::async_trait]
impl QueryPlanner for HoldingQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(HeldOrderPlanner)])
            .create_physical_plan(logical_plan, session)
            .await
    }
}

/// Plans a held order to its execution plan.
#[derive(Debug)]
struct HeldOrderPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for HeldOrderPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(held) = node.as_any().downcast_ref::<HeldOrder>() else {
            return Ok(None);
        };
        let ([logical_input], [physical_input]) = (logical_inputs, physical_inputs) else {
            return Err(DataFusionError::Internal(format!(
                "{} takes one input",
                held.label()
            )));
        };
        let requirements = held
            .ordering
            .iter()
            .map(|sort| {
                let expr = planner.create_physical_expr(
                    &sort.expr,
                    logical_input.schema(),
                    session,
                    planning_ctx,
                )?;
                Ok(PhysicalSortRequirement::new(
                    expr,
                    Some(SortOptions::new(!sort.asc, sort.nulls_first)),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let Some(requirement) = LexRequirement::new(requirements) else {
            return Err(DataFusionError::Plan(format!(
                "{} needs at least one key",
                held.label()
            )));
        };
        let verify = session.config().get_extension::<VerifyOrder>().is_some();
        Ok(Some(Arc::new(HeldOrderExec::new(
            Arc::clone(physical_input),
            requirement,
            held.kind,
            held.scope,
            verify,
        ))))
    }
}

/// The execution plan of an assertion, `OrderingAssertionExec`, or of an established order,
/// `EstablishedOrderExec`. It passes its input's rows on unchanged.
#[derive(Debug)]
pub struct HeldOrderExec {
    input: Arc<dyn ExecutionPlan>,
    requirement: LexRequirement,
    kind: Kind,
    scope: AssertionScope,
    verify: bool,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl HeldOrderExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        requirement: LexRequirement,
        kind: Kind,
        scope: AssertionScope,
        verify: bool,
    ) -> Self {
        let cache = Arc::new(input.properties().as_ref().clone());
        Self {
            input,
            requirement,
            kind,
            scope,
            verify,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    fn keys(&self) -> String {
        self.requirement
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Walks the plan beneath this assertion along the edges that carry its order, failing at the
    /// first node that sorts or that keeps none of its inputs' orders.
    fn check(&self) -> Result<()> {
        let mut path = vec![self.name().to_string()];
        check_beneath(&self.input, &mut path).map_err(|(reason, path)| {
            DataFusionError::External(Box::new(OrderingAssertionFailed {
                message: format!(
                    "ordering assertion [{}] failed: the plan can meet it only {reason}; path: {}",
                    self.keys(),
                    path.join(" -> ")
                ),
            }))
        })
    }
}

/// Checks `node` and the nodes beneath it that carry its order, `path` naming the nodes from the
/// assertion down to it. A failure names why and the path down to the node that failed.
fn check_beneath(
    node: &Arc<dyn ExecutionPlan>,
    path: &mut Vec<String>,
) -> Result<(), (String, Vec<String>)> {
    if node.is::<HeldOrderExec>() {
        return Ok(());
    }
    path.push(node.name().to_string());
    if node.is::<SortExec>() || node.is::<PartialSortExec>() || node.is::<PartitionedTopKExec>() {
        return Err((format!("by sorting, with {}", one_line(node)), path.clone()));
    }
    let children = node.children();
    if children.is_empty() {
        path.pop();
        return Ok(());
    }
    let every_input = node.is::<UnionExec>() || node.is::<SortPreservingMergeExec>();
    let kept = node.maintains_input_order();
    if !every_input && !kept.iter().any(|kept| *kept) {
        return Err((
            format!(
                "through a node that keeps no input's order, {}, so it fails closed",
                one_line(node)
            ),
            path.clone(),
        ));
    }
    for (child, kept) in children.into_iter().zip(kept) {
        if every_input || kept {
            check_beneath(child, path)?;
        }
    }
    path.pop();
    Ok(())
}

/// `node` as one line of a rendered plan.
fn one_line(node: &Arc<dyn ExecutionPlan>) -> String {
    struct OneLine<'a>(&'a dyn ExecutionPlan);
    impl fmt::Display for OneLine<'_> {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_as(DisplayFormatType::Default, formatter)
        }
    }
    OneLine(node.as_ref()).to_string()
}

impl DisplayAs for HeldOrderExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: order=[{}]", self.name(), self.keys())?;
        if self.scope == AssertionScope::EachPartition {
            formatter.write_str(", each partition")?;
        }
        if self.verify {
            formatter.write_str(", verify_order")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for HeldOrderExec {
    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Asserted => "OrderingAssertionExec",
            Kind::Established => "EstablishedOrderExec",
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![match self.scope {
            AssertionScope::OnePartition => Distribution::SinglePartition,
            AssertionScope::EachPartition => Distribution::UnspecifiedDistribution,
        }])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::new(self.requirement.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Internal(format!(
                "{} takes one child, got {}",
                self.name(),
                children.len()
            )));
        };
        Ok(Arc::new(Self::new(
            Arc::clone(input),
            self.requirement.clone(),
            self.kind,
            self.scope,
            self.verify,
        )))
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
        visit: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for requirement in &self.requirement {
            if visit(&requirement.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        match (check, self.kind) {
            (InvariantLevel::Executable, Kind::Asserted) => self.check(),
            (InvariantLevel::Executable, Kind::Established) | (InvariantLevel::Always, _) => Ok(()),
        }
    }

    /// Passes the input's rows on, recording this node's baseline metrics and, with the run-time
    /// order check on, checking their order.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let mut input = if self.verify {
            self.verified(input, partition, baseline.clone())?
        } else {
            input
        };
        let metered = stream::poll_fn(move |cx| baseline.record_poll(input.poll_next_unpin(cx)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, metered)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl HeldOrderExec {
    /// `input`, partition `partition` of this node's input, failing at the first row that is out
    /// of this node's order. The check's time counts as `baseline`'s compute time.
    fn verified(
        &self,
        input: SendableRecordBatchStream,
        partition: usize,
        baseline: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        let schema = input.schema();
        let exprs = self
            .requirement
            .iter()
            .map(|requirement| Arc::clone(&requirement.expr))
            .collect::<Vec<_>>();
        let fields = self
            .requirement
            .iter()
            .map(|requirement| {
                Ok(SortField::new_with_options(
                    requirement.expr.data_type(&schema)?,
                    requirement.options.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(fields)?;
        let name = self.name().to_string();
        let keys = self.keys();
        let mut previous: Option<OwnedRow> = None;
        let mut seen = 0_usize;
        let verified = input.map(move |batch| {
            let batch = batch?;
            let _timer = baseline.elapsed_compute().timer();
            let columns = exprs
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<ArrayRef>>>()?;
            let rows = converter.convert_columns(&columns)?;
            for (index, row) in rows.iter().enumerate() {
                if previous
                    .as_ref()
                    .is_some_and(|previous| previous.row().cmp(&row) == Ordering::Greater)
                {
                    return Err(DataFusionError::Execution(format!(
                        "{name} [{keys}] failed at run time: row {} of partition {partition} is out of order",
                        seen.saturating_add(index)
                    )));
                }
                previous = Some(row.owned());
            }
            seen = seen.saturating_add(batch.num_rows());
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, verified)))
    }
}
