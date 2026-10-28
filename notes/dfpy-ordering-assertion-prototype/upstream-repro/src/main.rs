//! `pushdown_sorts` moves a `SortExec` beneath a custom node that requires a different input
//! order, whenever the two orders agree on any key at the same position.
//! `pushdown_would_violate_requirements` blocks the push only if *every* key pair is incompatible.
//! `SanityCheckPlan` then rejects the plan.
//!
//! The plan: `SortExec[a, b, c]` over `NeedsOrder[<required>]` over a table sorted by `(a, b, c)`.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream, displayable,
};
use datafusion::prelude::*;

/// Requires its input in `required` order and keeps that order, like a sink or an assertion.
#[derive(Debug)]
struct NeedsOrder {
    input: Arc<dyn ExecutionPlan>,
    required: LexRequirement,
    cache: Arc<PlanProperties>,
}

impl NeedsOrder {
    fn new(input: Arc<dyn ExecutionPlan>, required: LexRequirement) -> Self {
        let cache = Arc::new(input.properties().as_ref().clone());
        Self { input, required, cache }
    }
}

impl DisplayAs for NeedsOrder {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let keys = self.required.iter().map(ToString::to_string).collect::<Vec<_>>();
        write!(f, "NeedsOrder: [{}]", keys.join(", "))
    }
}

impl ExecutionPlan for NeedsOrder {
    fn name(&self) -> &'static str {
        "NeedsOrder"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::new(self.required.clone()))]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]), self.required.clone())))
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(children, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

async fn run(required: &[&str]) -> Result<()> {
    let schema = Arc::new(Schema::new(
        ["a", "b", "c"].map(|n| Field::new(n, DataType::Int32, false)).to_vec(),
    ));
    let column = |v: [i32; 4]| Arc::new(Int32Array::from(v.to_vec())) as _;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![column([1, 1, 2, 2]), column([1, 2, 1, 2]), column([1, 1, 1, 1])],
    )?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", batch)?;
    let scan = ctx.sql("SELECT a, b, c FROM t ORDER BY a, b, c").await?.create_physical_plan().await?;

    let requirement = |names: &[&str]| -> Result<LexRequirement> {
        let reqs = names
            .iter()
            .map(|n| Ok(PhysicalSortRequirement::new(col(n, &schema)?, Some(Default::default()))))
            .collect::<Result<Vec<_>>>()?;
        Ok(LexRequirement::new(reqs).unwrap())
    };
    let outer = LexOrdering::new(
        ["a", "b", "c"]
            .iter()
            .map(|n| Ok(PhysicalSortExpr::new(col(n, &schema)?, Default::default())))
            .collect::<Result<Vec<_>>>()?,
    )
    .unwrap();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
        outer,
        Arc::new(NeedsOrder::new(scan, requirement(required)?)),
    ));
    println!("\n=== NeedsOrder requires {required:?}; the sort above it is [a, b, c]");
    println!("{}", displayable(plan.as_ref()).indent(true));

    let config = ctx.state().config_options().as_ref().clone();
    let mut plan = plan;
    for rule in PhysicalOptimizer::new().rules {
        match rule.optimize(Arc::clone(&plan), &config) {
            Ok(optimized) => plan = optimized,
            Err(e) => {
                println!("plan before {}:\n{}", rule.name(), displayable(plan.as_ref()).indent(true));
                println!("ERROR in {}: {e}", rule.name());
                return Ok(());
            }
        }
    }
    println!("optimized:\n{}", displayable(plan.as_ref()).indent(true));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Every key pair incompatible: the guard blocks the push and the plan is valid.
    run(&["b", "a"]).await?;
    // The same, plus a shared third key: the guard lets the sort through and the plan is invalid.
    run(&["b", "a", "c"]).await?;
    Ok(())
}
