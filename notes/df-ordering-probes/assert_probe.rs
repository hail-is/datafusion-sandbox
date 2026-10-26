use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::{
    LexRequirement, OrderingRequirements, PhysicalExpr, PhysicalSortRequirement,
};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::execution_plan::InvariantLevel;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    InputDistributionRequirements, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, displayable,
};
use datafusion::prelude::*;
use datafusion::logical_expr::SortExpr;

#[derive(Debug)]
struct AssertExec {
    input: Arc<dyn ExecutionPlan>,
    req: LexRequirement,
    cache: Arc<PlanProperties>,
}

impl AssertExec {
    fn new(input: Arc<dyn ExecutionPlan>, req: LexRequirement) -> Self {
        let cache = Arc::new(input.properties().as_ref().clone());
        Self { input, req, cache }
    }
}

impl DisplayAs for AssertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let ks: Vec<String> = self.req.iter().map(|r| r.expr.to_string()).collect();
        write!(f, "AssertExec: req=[{}]", ks.join(", "))
    }
}

impl ExecutionPlan for AssertExec {
    fn name(&self) -> &'static str {
        "AssertExec"
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
        vec![Some(OrderingRequirements::new(self.req.clone()))]
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
        _o: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AssertExec::new(
            Arc::clone(&children[0]),
            self.req.clone(),
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
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        if matches!(check, InvariantLevel::Executable) {
            let mut sorts = vec![];
            self.input.apply(|n| {
                let name = n.name();
                if name.contains("Sort") && name != "SortPreservingMergeExec" {
                    sorts.push(name.to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            println!("    [check_invariants(Executable) on AssertExec: sorts below = {sorts:?}]");
        }
        Ok(())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("alleles", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(start: i32, n: i32) -> RecordBatch {
    let pos: Vec<i32> = (start..start + n).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec!["1"; n as usize])),
            Arc::new(Int32Array::from(pos.clone())),
            Arc::new(StringArray::from(vec!["A/T"; n as usize])),
            Arc::new(Int64Array::from(
                pos.iter().map(|p| *p as i64 * 7 % 13).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn sort_order() -> Vec<Vec<SortExpr>> {
    vec![vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
        col("alleles").sort(true, false),
    ]]
}

fn plan_str(p: &Arc<dyn ExecutionPlan>) -> String {
    displayable(p.as_ref()).indent(false).to_string()
}

async fn make_ctx(target_partitions: usize, prefer_existing_sort: bool) -> Result<SessionContext> {
    let mut cfg = SessionConfig::new().with_target_partitions(target_partitions);
    if std::env::var("NO_SORT_PUSHDOWN").is_ok() { cfg.options_mut().optimizer.enable_sort_pushdown = false; }
    cfg.options_mut().optimizer.prefer_existing_sort = prefer_existing_sort;
    let state = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features()
        .with_physical_optimizer_rules(vec![])
        .build();
    let ctx = SessionContext::new_with_state(state);
    let t = MemTable::try_new(schema(), vec![vec![batch(0, 1000)]])?.with_sort_order(sort_order());
    ctx.register_table("t", Arc::new(t))?;
    let t2 = MemTable::try_new(schema(), vec![vec![batch(500, 1000)]])?.with_sort_order(sort_order());
    ctx.register_table("t2", Arc::new(t2))?;
    let big = MemTable::try_new(schema(), vec![vec![batch(0, 50000)]])?.with_sort_order(sort_order());
    ctx.register_table("big", Arc::new(big))?;
    // A struct-keyed table ordered by the struct column itself.
    let sbatch = batch(0, 1000);
    let locus = datafusion::arrow::array::StructArray::from(vec![
        (Arc::new(Field::new("contig", DataType::Utf8, false)), Arc::clone(sbatch.column(0))),
        (Arc::new(Field::new("position", DataType::Int32, false)), Arc::clone(sbatch.column(1))),
    ]);
    let sschema = Arc::new(Schema::new(vec![
        Field::new("locus", locus.data_type().clone(), false),
        Field::new("alleles", DataType::Utf8, false),
    ]));
    let sb = RecordBatch::try_new(Arc::clone(&sschema), vec![Arc::new(locus), Arc::clone(sbatch.column(2))])?;
    let st = MemTable::try_new(sschema, vec![vec![sb]])?
        .with_sort_order(vec![vec![col("locus").sort(true, false), col("alleles").sort(true, false)]]);
    ctx.register_table("st", Arc::new(st))?;
    // Two parquet files whose alphabetical order is the reverse of key order.
    let dir = "/tmp/df-ordering-probe-data/p";
    std::fs::create_dir_all(dir).unwrap();
    for (name, start) in [("a.parquet", 100), ("b.parquet", 0)] {
        let file = std::fs::File::create(format!("{dir}/{name}")).unwrap();
        let mut w = ArrowWriter::try_new(file, schema(), None)?;
        w.write(&batch(start, 100))?;
        w.close()?;
    }
    ctx.register_parquet(
        "p",
        dir,
        ParquetReadOptions::default().file_sort_order(sort_order()),
    )
    .await?;
    Ok(ctx)
}

async fn scenario(
    title: &str,
    target_partitions: usize,
    prefer_existing_sort: bool,
    sql: &str,
    keys: &[&str],
) -> Result<()> {
    println!("\n=== {title}  (target_partitions={target_partitions}, prefer_existing_sort={prefer_existing_sort})");
    println!("SQL: {sql}\nassert keys: {keys:?}");
    let ctx = make_ctx(target_partitions, prefer_existing_sort).await?;
    let df = ctx.sql(sql).await?;
    let logical = df.into_optimized_plan()?;
    let state = ctx.state();
    let initial = state.create_physical_plan(&logical).await?;
    let df_schema = logical.schema();
    let mut reqs = vec![];
    for k in keys {
        let e = ctx.parse_sql_expr(k, df_schema)?;
        let pe = state.create_physical_expr(e, df_schema)?;
        reqs.push(PhysicalSortRequirement::new(
            pe,
            Some(datafusion::arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            }),
        ));
    }
    let req = LexRequirement::new(reqs).unwrap();
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(AssertExec::new(initial, req));
    println!("-- initial:\n{}", plan_str(&plan));
    let config: ConfigOptions = state.config_options().as_ref().clone();
    let mut prev = plan_str(&plan);
    for rule in PhysicalOptimizer::new().rules {
        match rule.optimize(Arc::clone(&plan), &config) {
            Ok(p) => plan = p,
            Err(e) => {
                println!("-- rule {} ERROR: {e}", rule.name());
                return Ok(());
            }
        }
        let s = plan_str(&plan);
        if s != prev {
            println!("-- after {}:\n{}", rule.name(), s);
            prev = s;
        }
    }
    // Mimic the planner's final InvariantChecker(Executable) walk.
    plan.apply(|n| {
        n.check_invariants(InvariantLevel::Executable)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let only = std::env::args().nth(1);
    if only.as_deref() == Some("agg") {
        scenario("I3 big group by keys", 4, false,
            "SELECT contig, position, alleles, sum(v) AS s FROM big GROUP BY contig, position, alleles",
            &["contig", "position", "alleles"]).await?;
        scenario("I4 big group by keys, prefer_existing_sort", 4, true,
            "SELECT contig, position, alleles, sum(v) AS s FROM big GROUP BY contig, position, alleles",
            &["contig", "position", "alleles"]).await?;
        return Ok(());
    }
    if only.as_deref() == Some("extra") {
        scenario("E4 get_field on struct-ordered source", 4, false,
            "SELECT locus['contig'] AS contig, locus['position'] AS position, alleles FROM st", &["contig", "position", "alleles"]).await?;
        scenario("E5 struct-ordered source, key = struct col", 4, false,
            "SELECT locus, alleles FROM st", &["locus", "alleles"]).await?;
        scenario("G3 big filter tp=4", 4, false,
            "SELECT contig, position, alleles FROM big WHERE v > 3", &["contig", "position", "alleles"]).await?;
        scenario("G4 big filter tp=4 prefer_existing_sort", 4, true,
            "SELECT contig, position, alleles FROM big WHERE v > 3", &["contig", "position", "alleles"]).await?;
        scenario("H3 union, one branch ordered longer by explicit ORDER BY", 4, false,
            "SELECT contig, position, alleles, v FROM t UNION ALL SELECT * FROM (SELECT contig, position, alleles, v FROM t2 ORDER BY contig, position, alleles, v)", &["contig", "position", "alleles"]).await?;
        scenario("L parquet out-of-order, enable_sort_pushdown=false", 1, false,
            "SELECT contig, position, alleles, v FROM p", &["contig", "position", "alleles"]).await?;
        return Ok(());
    }
    let base = "SELECT contig, position, alleles, v FROM t";
    scenario("A plain scan", 4, false, base, &["contig", "position", "alleles"]).await?;
    scenario("A1 plain scan, tp=1", 1, false, base, &["contig", "position", "alleles"]).await?;
    scenario("B position + 1", 4, false,
        "SELECT contig, position + 1 AS position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("C widening CAST", 4, false,
        "SELECT contig, CAST(position AS BIGINT) AS position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("C2 narrowing CAST", 4, false,
        "SELECT contig, CAST(position AS SMALLINT) AS position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("C3 CAST to Utf8", 4, false,
        "SELECT contig, CAST(position AS VARCHAR) AS position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("D aliases", 4, false,
        "SELECT contig AS c, position AS p, alleles AS a FROM t", &["c", "p", "a"]).await?;
    scenario("E named_struct, keys via get_field", 4, false,
        "SELECT named_struct('contig', contig, 'position', position) AS locus, alleles FROM t",
        &["locus['contig']", "locus['position']", "alleles"]).await?;
    scenario("E2 named_struct, key is the struct column", 4, false,
        "SELECT named_struct('contig', contig, 'position', position) AS locus, alleles FROM t",
        &["locus", "alleles"]).await?;
    scenario("E3 get_field from a struct-ordered input", 4, false,
        "SELECT locus['contig'] AS contig, locus['position'] AS position, alleles FROM (SELECT named_struct('contig', contig, 'position', position) AS locus, alleles FROM t)",
        &["contig", "position", "alleles"]).await?;
    scenario("F concat", 4, false,
        "SELECT concat(contig, ':') AS contig, position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("F2 floor on second key", 4, false,
        "SELECT contig, floor(position) AS position, alleles FROM t", &["contig", "position", "alleles"]).await?;
    scenario("G filter on v", 4, false,
        "SELECT contig, position, alleles FROM t WHERE v > 3", &["contig", "position", "alleles"]).await?;
    scenario("G2 filter contig = lit, drop contig", 4, false,
        "SELECT position, alleles FROM t WHERE contig = '1'", &["position", "alleles"]).await?;
    scenario("H union all", 4, false,
        "SELECT contig, position, alleles FROM t UNION ALL SELECT contig, position, alleles FROM t2",
        &["contig", "position", "alleles"]).await?;
    scenario("H2 union all, tp=1", 1, false,
        "SELECT contig, position, alleles FROM t UNION ALL SELECT contig, position, alleles FROM t2",
        &["contig", "position", "alleles"]).await?;
    scenario("I group by keys", 4, false,
        "SELECT contig, position, alleles, sum(v) AS s FROM t GROUP BY contig, position, alleles",
        &["contig", "position", "alleles"]).await?;
    scenario("I2 group by keys, prefer_existing_sort", 4, true,
        "SELECT contig, position, alleles, sum(v) AS s FROM t GROUP BY contig, position, alleles",
        &["contig", "position", "alleles"]).await?;
    scenario("J parquet, files listed out of key order", 1, false,
        "SELECT contig, position, alleles, v FROM p", &["contig", "position", "alleles"]).await?;
    scenario("J2 parquet, files listed out of key order, tp=4", 4, false,
        "SELECT contig, position, alleles, v FROM p", &["contig", "position", "alleles"]).await?;
    scenario("K ORDER BY v LIMIT under the assertion", 4, false,
        "SELECT * FROM (SELECT contig, position, alleles, v FROM t ORDER BY v LIMIT 10)", &["contig", "position", "alleles"]).await?;
    Ok(())
}
