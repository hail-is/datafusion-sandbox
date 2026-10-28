//! PROTOTYPE, throwaway, for hail-is/datafusion-sandbox#322: the ordering assertion.
//!
//! - `assert_ordered`: the marker UDF the frontend calls inside `Filter(assert_ordered(...))`.
//!   Its arguments alternate a key expression and a literal like `'ASC NULLS LAST'`.
//! - `lift_assertions`: the backend's replacement pass, run with the scan reopen before the
//!   logical optimizer, turning each marker filter into an `OrderingAssertion` node.
//! - `OrderingAssertion`: the backend-only logical node. It never crosses FFI.
//! - `AssertionPlanner`: the extension planner that plans it into `AssertExec`.
//! - `AssertExec`: requires one partition in the asserted ordering, as `DataSinkExec` does, and
//!   in `check_invariants(Executable)` fails if the final plan meets the requirement by sorting.
//!   With `verify` set it also checks the order of the rows it passes on.
//! - An established order: the replacement pass also wraps every frontend `Sort` in an
//!   `OrderingAssertion` marked `established`. It plans to `EstablishedOrderExec`, which requires
//!   the same as an assertion but accepts the sort beneath it. An assertion's check stops there:
//!   a sort the frontend wrote is expected, only a sort the backend inserted is a failure.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    DFSchemaRef, DataFusionError, Result, ScalarValue, exec_err, internal_err, plan_err,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, Extension, Filter, LogicalPlan, Sort, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, SortExpr, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
    Volatility,
};
use datafusion::physical_expr::{LexRequirement, OrderingRequirements, PhysicalSortRequirement};
use datafusion::physical_plan::execution_plan::InvariantLevel;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    InputDistributionRequirements, PhysicalExpr, PlanProperties, ReplaceChildrenOptions,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_session::QueryPlanner;
use futures::StreamExt;

pub const ASSERT_ORDERED: &str = "assert_ordered";

// ---------------------------------------------------------------------------------------------
// The marker UDF. Registered on both sides under the same name; it only ever evaluates to true.

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AssertOrderedUdf {
    signature: Signature,
}

pub fn assert_ordered_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(AssertOrderedUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
}

impl ScalarUDFImpl for AssertOrderedUdf {
    fn name(&self) -> &str {
        ASSERT_ORDERED
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
    }
}

// ---------------------------------------------------------------------------------------------
// The replacement pass.

fn is_marker(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(ScalarFunction { func, .. }) if func.name() == ASSERT_ORDERED)
}

/// The asserted ordering from a marker call's arguments: `key, 'ASC NULLS LAST', key, ...`.
fn ordering_of(args: &[Expr]) -> Result<Vec<SortExpr>> {
    if args.is_empty() || args.len() % 2 != 0 {
        return plan_err!("{ASSERT_ORDERED} takes pairs of a key and its direction");
    }
    args.chunks(2)
        .map(|pair| {
            // The wheel's `lit("...")` is a Utf8View.
            let Expr::Literal(value, _) = &pair[1] else {
                return plan_err!("{ASSERT_ORDERED}: '{}' is not a direction literal", pair[1]);
            };
            let Some(Some(direction)) = value.try_as_str() else {
                return plan_err!("{ASSERT_ORDERED}: '{}' is not a direction literal", pair[1]);
            };
            let (asc, nulls_first) = match direction {
                "ASC NULLS LAST" => (true, false),
                "ASC NULLS FIRST" => (true, true),
                "DESC NULLS LAST" => (false, false),
                "DESC NULLS FIRST" => (false, true),
                other => return plan_err!("{ASSERT_ORDERED}: unknown direction '{other}'"),
            };
            Ok(SortExpr::new(pair[0].clone(), asc, nulls_first))
        })
        .collect()
}

/// Replaces every `Filter(assert_ordered(...))` with an `OrderingAssertion`. A marker anywhere
/// else (inside a larger predicate, a projection, a join filter) is an error, so a frontend bug
/// can't silently turn an assertion into a no-op filter.
pub fn lift_assertions(plan: LogicalPlan, verify: bool) -> Result<LogicalPlan> {
    let plan = plan
        .transform_up(|node| {
            if let LogicalPlan::Filter(Filter {
                predicate: Expr::ScalarFunction(ScalarFunction { func, args }),
                input,
                ..
            }) = &node
                && func.name() == ASSERT_ORDERED
            {
                let assertion = OrderingAssertion {
                    input: Arc::clone(input),
                    ordering: ordering_of(args)?,
                    verify,
                    established: false,
                };
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(assertion),
                })));
            }
            if let LogicalPlan::Sort(Sort { expr, .. }) = &node {
                let established = OrderingAssertion {
                    ordering: expr.clone(),
                    input: Arc::new(node),
                    verify,
                    established: true,
                };
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(established),
                })));
            }
            Ok(Transformed::no(node))
        })?
        .data;
    plan.apply(|node| {
        for expr in node.expressions() {
            if expr.exists(|e| Ok(is_marker(e)))? {
                return plan_err!("{ASSERT_ORDERED} outside a filter of its own: {expr}");
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(plan)
}

/// Wraps `plan` in an assertion of `ordering`: the sink's requirement, stated as an assertion.
pub fn assert_at_root(plan: LogicalPlan, ordering: Vec<SortExpr>, verify: bool) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(OrderingAssertion {
            input: Arc::new(plan),
            ordering,
            verify,
            established: false,
        }),
    })
}

// ---------------------------------------------------------------------------------------------
// The backend-only logical node.

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct OrderingAssertion {
    input: Arc<LogicalPlan>,
    ordering: Vec<SortExpr>,
    verify: bool,
    /// An order a frontend sort established, rather than one the frontend asserts.
    established: bool,
}

impl UserDefinedLogicalNodeCore for OrderingAssertion {
    fn name(&self) -> &str {
        "OrderingAssertion"
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

    /// A filter keeps row order, so filters may move below the assertion.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let keys = self.ordering.iter().map(ToString::to_string).collect::<Vec<_>>();
        let name = if self.established { "EstablishedOrder" } else { "OrderingAssertion" };
        write!(f, "{name}: {}", keys.join(", "))
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        let Some(input) = inputs.pop() else {
            return internal_err!("OrderingAssertion has one input");
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
            verify: self.verify,
            established: self.established,
        })
    }

    /// Projection pushdown may prune columns below the assertion, keeping its keys.
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
}

// ---------------------------------------------------------------------------------------------
// Planning.

#[derive(Debug)]
pub struct AssertionPlanner;

#[async_trait]
impl ExtensionPlanner for AssertionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(assertion) = node.as_any().downcast_ref::<OrderingAssertion>() else {
            return Ok(None);
        };
        let schema = logical_inputs[0].schema();
        let requirements = assertion
            .ordering
            .iter()
            .map(|sort| {
                let expr = planner.create_physical_expr(&sort.expr, schema, session, planning_ctx)?;
                Ok(PhysicalSortRequirement::new(
                    expr,
                    Some(SortOptions {
                        descending: !sort.asc,
                        nulls_first: sort.nulls_first,
                    }),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let Some(requirement) = LexRequirement::new(requirements) else {
            return plan_err!("an ordering assertion needs at least one key");
        };
        Ok(Some(Arc::new(AssertExec::new(
            Arc::clone(&physical_inputs[0]),
            requirement,
            assertion.verify,
            assertion.established,
        ))))
    }
}

/// The backend's query planner: the default planner plus the assertion's extension planner.
#[derive(Debug)]
pub struct BackendQueryPlanner;

#[async_trait]
impl QueryPlanner for BackendQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(AssertionPlanner)])
            .create_physical_plan(logical_plan, session)
            .await
    }
}

// ---------------------------------------------------------------------------------------------
// The execution plan and its check.

#[derive(Debug)]
pub struct AssertExec {
    input: Arc<dyn ExecutionPlan>,
    requirement: LexRequirement,
    verify: bool,
    established: bool,
    cache: Arc<PlanProperties>,
}

impl AssertExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        requirement: LexRequirement,
        verify: bool,
        established: bool,
    ) -> Self {
        let cache = Arc::new(input.properties().as_ref().clone());
        Self {
            input,
            requirement,
            verify,
            established,
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

    /// Walks the order-carrying path below the assertion and fails on the first node that sorts,
    /// or on a node that claims the order without keeping its input's (fail closed).
    fn find_sort(&self) -> Result<()> {
        let mut path = vec![self.name().to_string()];
        walk(&self.input, &mut path).map_err(|(node, path)| {
            DataFusionError::Plan(format!(
                "ordering assertion [{}] failed: the plan can only meet it by sorting.\n  {node}\n  \
                 path: {}",
                self.keys(),
                path.join(" -> ")
            ))
        })
    }
}

const SORTING: [&str; 3] = ["SortExec", "PartialSortExec", "PartitionedTopKExec"];
/// Nodes below which another node already answers for the order: an assertion checks its own
/// subtree, and an established order is a sort the frontend asked for.
const ANSWERED: [&str; 2] = ["OrderingAssertionExec", "EstablishedOrderExec"];

/// `Err((reason, path))` names the node that sorted and the path down to it.
fn walk(node: &Arc<dyn ExecutionPlan>, path: &mut Vec<String>) -> Result<(), (String, Vec<String>)> {
    let name = node.name();
    path.push(name.to_string());
    if ANSWERED.contains(&name) {
        path.pop();
        return Ok(());
    }
    if SORTING.iter().any(|sorting| name.starts_with(sorting)) {
        let line = one_line(node);
        return Err((format!("sorted by: {line}"), path.clone()));
    }
    let children = node.children();
    if children.is_empty() {
        path.pop();
        return Ok(());
    }
    let all = name == "UnionExec" || name == "SortPreservingMergeExec";
    let maintains = node.maintains_input_order();
    if !all && !maintains.iter().any(|kept| *kept) {
        let line = one_line(node);
        return Err((
            format!("ordered by a node that keeps no input's order (unknown, failing closed): {line}"),
            path.clone(),
        ));
    }
    for (child, kept) in children.into_iter().zip(maintains) {
        if all || kept {
            walk(child, path)?;
        }
    }
    path.pop();
    Ok(())
}

fn one_line(node: &Arc<dyn ExecutionPlan>) -> String {
    struct One<'a>(&'a dyn ExecutionPlan);
    impl fmt::Display for One<'_> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.fmt_as(DisplayFormatType::Default, f)
        }
    }
    One(node.as_ref()).to_string()
}

impl DisplayAs for AssertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: [{}]", self.name(), self.keys())?;
        if self.verify {
            write!(f, ", verify=true")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for AssertExec {
    fn name(&self) -> &'static str {
        if self.established {
            "EstablishedOrderExec"
        } else {
            "OrderingAssertionExec"
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::SinglePartition])
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

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(input) = children.pop() else {
            return internal_err!("OrderingAssertionExec has one child");
        };
        Ok(Arc::new(Self::new(
            input,
            self.requirement.clone(),
            self.verify,
            self.established,
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
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for requirement in self.requirement.iter() {
            if f(&requirement.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        match check {
            InvariantLevel::Executable if !self.established => self.find_sort(),
            InvariantLevel::Executable => Ok(()),
            InvariantLevel::Always => Ok(()),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        if !self.verify {
            return Ok(stream);
        }
        let schema = stream.schema();
        let exprs = self
            .requirement
            .iter()
            .map(|r| Arc::clone(&r.expr))
            .collect::<Vec<_>>();
        let fields = self
            .requirement
            .iter()
            .map(|r| {
                Ok(SortField::new_with_options(
                    r.expr.data_type(&schema)?,
                    r.options.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(fields)?;
        let keys = self.keys();
        let mut previous: Option<OwnedRow> = None;
        let mut seen = 0usize;
        let checked = stream.map(move |batch| {
            let batch = batch?;
            let columns = exprs
                .iter()
                .map(|e| e.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<ArrayRef>>>()?;
            let rows = converter.convert_columns(&columns)?;
            for (i, row) in rows.iter().enumerate() {
                if let Some(prev) = &previous
                    && prev.row().cmp(&row) == Ordering::Greater
                {
                    return exec_err!(
                        "ordering assertion [{keys}] failed at run time: row {} is out of order",
                        seen + i
                    );
                }
                previous = Some(row.owned());
            }
            seen += batch.num_rows();
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
    }
}
