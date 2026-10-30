//! Ordering assertions and established orders: what they hold a plan to, and when planning fails.
//!
//! Every test here plans with the run-time order check on, so a plan whose assertion passes but
//! whose rows arrive out of order fails as it executes.

use crate::{
    locus::LocusRepresentation,
    ordering_assertion::{self, AssertionScope, OrderingAssertionFailed},
    pipeline::{self, PipelineOptions},
    row_ordering::SortColumn,
    testing::{
        fixture::{self, FixtureFormat, block_on},
        formulation_plans::{self, FORMATS, FixtureDataset},
        plan_shape::PlanShape,
        session::hostile_config,
    },
};

use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    common::{JoinType, tree_node::TreeNodeRecursion},
    datasource::MemTable,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableType},
    physical_expr::PhysicalExpr,
    physical_plan::{
        self, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
        ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions, sorts::sort::SortExec,
    },
    prelude::{DataFrame, SessionConfig, SessionContext, col, lit},
};

use std::{fmt, sync::Arc};

/// The sessions every shape is checked under: the shared defaults, and the hostile session that
/// splits any file scan it may.
fn sessions() -> [(&'static str, SessionConfig); 2] {
    [
        (
            "default",
            ordering_assertion::verify_order(pipeline::session_config()),
        ),
        (
            "hostile",
            ordering_assertion::verify_order(hostile_config(8)),
        ),
    ]
}

/// The fixture dataset of single-sample input tables in `format`, under contig-position loci.
fn dataset(format: FixtureFormat) -> FixtureDataset {
    formulation_plans::dataset(format, LocusRepresentation::ContigPosition)
}

/// The dataset's key order as a plain sort list: contig, position, alleles.
fn key_order(dataset: &FixtureDataset) -> Vec<SortColumn> {
    dataset
        .dataset
        .query_ordering(&crate::row_ordering::RowOrdering::locus_then_alleles())
        .unwrap()
        .sort_columns()
}

/// The flat union of every input table's scan, with no sort above it, on a session of `config`.
async fn flat_read(dataset: &FixtureDataset, config: SessionConfig) -> DataFrame {
    let ctx = SessionContext::new_with_config(config);
    dataset.fixture.register(&ctx);
    dataset.dataset.read(&ctx).await.unwrap()
}

/// The physical plan of `frame` held in `ordering` at its root over one partition.
async fn held_plan(frame: DataFrame, ordering: &[SortColumn]) -> Result<Arc<dyn ExecutionPlan>> {
    ordering_assertion::hold(frame, ordering, AssertionScope::OnePartition)?
        .create_physical_plan()
        .await
}

/// An assertion at the root of an unordered union of ordered scans holds the union with a merge,
/// as a sink's requirement did: no sort, under either session.
#[test]
fn a_root_assertion_over_ordered_scans_merges_them_without_a_sort() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        for (session, config) in sessions() {
            let plan = block_on(async {
                held_plan(flat_read(&dataset, config).await, &ordering)
                    .await
                    .unwrap()
            });

            let shape = PlanShape::of(&plan);
            assert_eq!(
                plan.name(),
                "OrderingAssertionExec",
                "{format:?}, {session}:\n{shape}"
            );
            shape.assert_merge_tree(&[dataset.input_tables.len()]);
        }
    }
}

/// The failed assertion planning `frame` held in `ordering` errs with, failing the test if
/// planning succeeds or fails otherwise.
fn assertion_failure(frame: DataFrame, ordering: &[SortColumn]) -> String {
    let error = block_on(held_plan(frame, ordering)).expect_err("expected planning to fail");
    OrderingAssertionFailed::find(&error)
        .unwrap_or_else(|| panic!("expected a failed ordering assertion, got: {error}"))
        .to_string()
}

fn ascending(column: &str) -> SortColumn {
    SortColumn {
        column: column.to_string(),
        ascending: true,
        nulls_first: false,
    }
}

/// An order the scans lack, position before contig, could be met only by sorting, so planning
/// fails with a failed assertion that names the sort.
#[test]
fn an_order_the_data_lacks_fails_before_execution() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = ["position", "contig", "alleles"].map(ascending);
        for (session, config) in sessions() {
            let frame = block_on(flat_read(&dataset, config));

            let failure = assertion_failure(frame, &ordering);
            assert!(
                failure.contains("by sorting"),
                "{format:?}, {session}: {failure}"
            );
        }
    }
}

/// A key the plan computes, `position + 1` under the name `position`, is not the scans' order
/// to the optimizer, so holding it fails as an order the data lacks does.
#[test]
fn a_computed_key_fails_before_execution() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        for (session, config) in sessions() {
            let frame = block_on(flat_read(&dataset, config))
                .select(vec![
                    col("contig"),
                    (col("position") + lit(1)).alias("position"),
                    col("alleles"),
                    col("s"),
                ])
                .unwrap();

            let failure = assertion_failure(frame, &ordering);
            assert!(
                failure.contains("by sorting"),
                "{format:?}, {session}: {failure}"
            );
        }
    }
}

/// A plan builder's sort into an order the data lacks, sample first, is an established order:
/// the sort it needs is the one it wrote, so the assertion above it holds.
#[test]
fn a_plan_builders_sort_beneath_the_assertion_is_accepted() {
    for format in FORMATS {
        let dataset = dataset(format);
        let mut ordering = vec![ascending("s")];
        ordering.extend(key_order(&dataset));
        let sorts: Vec<_> = ordering.iter().map(SortColumn::sort_expression).collect();
        for (session, config) in sessions() {
            let plan = block_on(async {
                let frame = flat_read(&dataset, config)
                    .await
                    .sort(sorts.clone())
                    .unwrap();
                held_plan(frame, &ordering).await.unwrap()
            });

            let shape = PlanShape::of(&plan);
            assert!(
                !shape.nodes_of::<SortExec>().is_empty(),
                "{format:?}, {session}: expected the plan builder's sort:\n{shape}"
            );
        }
    }
}

/// A frame ending in a sort into the order its scans already have keeps its merge tree: the
/// optimizer replaces the sort with a merge beneath the established order, as it would beneath a
/// sink's requirement.
#[test]
fn a_frame_ending_in_a_sort_keeps_its_merge_tree() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        let sorts: Vec<_> = ordering.iter().map(SortColumn::sort_expression).collect();
        for (session, config) in sessions() {
            let plan = block_on(async {
                let frame = flat_read(&dataset, config)
                    .await
                    .sort(sorts.clone())
                    .unwrap();
                held_plan(frame, &ordering).await.unwrap()
            });

            let shape = PlanShape::of(&plan);
            assert_eq!(
                shape.operators()[..2],
                ["OrderingAssertionExec", "  EstablishedOrderExec"],
                "{format:?}, {session}:\n{shape}"
            );
            shape.assert_merge_tree(&[dataset.input_tables.len()]);
        }
    }
}

/// A sort the backend inserted above an established order fails the assertion above both: the
/// plan builder sorted by sample, then computed its position key, so the key order above that
/// needs a sort of its own.
#[test]
fn a_sort_the_backend_inserts_above_an_established_order_fails() {
    for format in FORMATS {
        let dataset = dataset(format);
        let mut ordering = vec![ascending("s")];
        ordering.extend(key_order(&dataset));
        let sorts: Vec<_> = ordering.iter().map(SortColumn::sort_expression).collect();
        for (session, config) in sessions() {
            let frame = block_on(flat_read(&dataset, config))
                .sort(sorts.clone())
                .unwrap()
                .select(vec![
                    col("s"),
                    col("contig"),
                    (col("position") + lit(1)).alias("position"),
                    col("alleles"),
                ])
                .unwrap();

            let failure = assertion_failure(frame, &ordering);
            assert!(
                failure.contains("by sorting"),
                "{format:?}, {session}: {failure}"
            );
        }
    }
}

/// An assertion over each partition holds every partition in its order and keeps them: no merge
/// joins the ordered scans, and no sort orders them.
#[test]
fn an_assertion_over_each_partition_keeps_the_partitions() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        for (session, config) in sessions() {
            let plan = block_on(async {
                let frame = flat_read(&dataset, config).await;
                ordering_assertion::hold(frame, &ordering, AssertionScope::EachPartition)
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap()
            });

            let shape = PlanShape::of(&plan);
            assert_eq!(
                plan.output_partitioning().partition_count(),
                dataset.input_tables.len(),
                "{format:?}, {session}:\n{shape}"
            );
            shape.assert_one_ordered_partition_per_sample(dataset.input_tables.len());
        }
    }
}

#[test]
fn an_assertion_needs_a_key() {
    let dataset = dataset(FixtureFormat::Parquet);
    let frame = block_on(flat_read(&dataset, pipeline::session_config()));

    let error = ordering_assertion::hold(frame, &[], AssertionScope::OnePartition).unwrap_err();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
}

/// The plan of `frame` held in `ordering`, planned, then executed: `Err` if planning fails, and
/// otherwise what executing it gave.
async fn planned_then_executed(
    frame: DataFrame,
    ordering: &[SortColumn],
) -> Result<Result<Vec<RecordBatch>>> {
    let frame = ordering_assertion::hold(frame, ordering, AssertionScope::OnePartition)?;
    let task_ctx = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    Ok(physical_plan::collect(plan, task_ctx).await)
}

/// With the run-time order check on, a held plan whose rows arrive in order runs as it would
/// without it: every row, in the key order.
#[test]
fn the_run_time_order_check_passes_rows_that_arrive_in_order() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        let batches = pipeline::run(
            move |_| async move {
                let frame = flat_read(
                    &dataset,
                    ordering_assertion::verify_order(hostile_config(8)),
                )
                .await;
                planned_then_executed(frame, &ordering).await?
            },
            PipelineOptions::single_threaded(),
        )
        .unwrap();

        let loci: Vec<_> = batches
            .iter()
            .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
            .collect();
        assert_eq!(loci.len(), 32, "{format:?}");
        assert!(loci.is_sorted(), "{format:?}: {loci:?}");
    }
}

/// A scan that declares an order its rows lack passes the planning check, since a scan is a leaf,
/// and the run-time order check fails execution at the first row out of order.
#[test]
fn the_run_time_order_check_fails_rows_that_arrive_out_of_order() {
    let outcome = pipeline::run(
        |_| async {
            let ctx = SessionContext::new_with_config(ordering_assertion::verify_order(
                pipeline::session_config(),
            ));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 3, 2]))],
            )?;
            let claimed = MemTable::try_new(schema, vec![vec![batch]])?
                .with_sort_order(vec![vec![col("x").sort(true, false)]]);
            let frame = ctx.read_table(Arc::new(claimed))?;
            planned_then_executed(frame, &[ascending("x")]).await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    let error = outcome.expect_err("expected execution to fail");
    assert!(
        error
            .to_string()
            .contains("row 2 of partition 0 is out of order"),
        "{error}"
    );
}

/// Expected failure while apache/datafusion#25052 reproduces: an inner sort-merge join on contig,
/// whose left key repeats, declares its output ordered by contig and then the right side's order,
/// so an assertion of that order plans with no sort, and the rows arrive out of it. Only the
/// run-time order check catches it. Tracked in issue #325.
#[test]
fn expected_failure_a_sort_merge_join_claims_an_order_its_rows_lack() {
    let dataset = dataset(FixtureFormat::Parquet);
    let outcome = pipeline::run(
        move |_| async move {
            let mut config = ordering_assertion::verify_order(pipeline::session_config())
                .with_target_partitions(4);
            config.options_mut().optimizer.prefer_hash_join = false;
            let ctx = SessionContext::new_with_config(config);
            dataset.fixture.register(&ctx);
            let read = |name: &str| dataset.dataset.restrict_to(&[name.to_string()]);
            let left = read(dataset.input_tables[0])?
                .read(&ctx)
                .await?
                .select(vec![col("contig"), col("s")])?;
            let right = read(dataset.input_tables[1])?
                .read(&ctx)
                .await?
                .select(vec![
                    col("contig").alias("r_contig"),
                    col("position").alias("r_position"),
                    col("alleles").alias("r_alleles"),
                ])?;
            let joined = left.join(right, JoinType::Inner, &["contig"], &["r_contig"], None)?;
            planned_then_executed(
                joined,
                &["contig", "r_position", "r_alleles"].map(ascending),
            )
            .await
        },
        PipelineOptions::single_threaded(),
    );

    let unfixed = matches!(
        &outcome,
        Ok(Err(error)) if error.to_string().contains("failed at run time")
    );
    assert!(
        unfixed,
        "apache/datafusion#25052 no longer reproduces: remove this test and check its box on \
         issue #325. Outcome: {outcome:?}"
    );
}

/// Expected failure while the gap in the guard apache/datafusion#11678 added remains, not yet
/// reported upstream: a plan builder sorts by (position, contig, alleles) and the root asserts
/// (contig, position, alleles). The sort the backend inserts above the established order should
/// fail the assertion, but the two orders agree on alleles, so `pushdown_sorts` pushes it beneath
/// the established order, and `SanityCheckPlan` rejects the plan with its own error first. The
/// plan still fails before execution. Tracked in issue #325.
#[test]
fn expected_failure_a_sort_pushed_beneath_an_established_order_fails_unclearly() {
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    let position_first = ["position", "contig", "alleles"].map(ascending);
    let sorts = position_first
        .iter()
        .map(SortColumn::sort_expression)
        .collect();
    let result = block_on(async {
        let frame = flat_read(
            &dataset,
            ordering_assertion::verify_order(pipeline::session_config()),
        )
        .await
        .sort(sorts)
        .unwrap();
        held_plan(frame, &ordering).await
    });

    let unfixed = matches!(
        &result,
        Err(error) if OrderingAssertionFailed::find(error).is_none()
            && error.to_string().contains("does not satisfy order requirements")
    );
    assert!(
        unfixed,
        "the sort pushdown past an established order no longer reproduces: remove this test, \
         check its box on issue #325, and expect a failed ordering assertion here instead. \
         Result: {result:?}"
    );
}

/// Expected failure while apache/datafusion#26745 reproduces: an order-preserving repartition
/// over a source declaring two orderings, `a` and `b`, merges its partitions on `(a, b)` but keeps
/// claiming `b`, so an assertion of `b` above a grouping on `(a, b)` plans with no sort and the
/// rows arrive out of it. Only the run-time order check catches it. The upstream reproduction,
/// with the files as in-memory partitions. Tracked in issue #325.
#[test]
fn expected_failure_an_order_preserving_repartition_claims_an_order_its_rows_lack() {
    let outcome = pipeline::run(
        |_| async {
            let ctx = SessionContext::new_with_config(
                ordering_assertion::verify_order(pipeline::session_config())
                    .with_target_partitions(2),
            );
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]));
            let file = |a: Vec<i32>, b: Vec<i32>| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
                )
            };
            let table = MemTable::try_new(
                Arc::clone(&schema),
                vec![
                    vec![file(vec![1, 2], vec![10, 20])?],
                    vec![file(vec![1, 3], vec![1, 2])?],
                ],
            )?
            .with_sort_order(vec![
                vec![col("a").sort(true, false)],
                vec![col("b").sort(true, false)],
            ]);
            let grouped = ctx.read_table(Arc::new(table))?.aggregate(
                vec![col("a"), col("b")],
                vec![datafusion::functions_aggregate::count::count_all()],
            )?;
            planned_then_executed(grouped, &[ascending("b")]).await
        },
        PipelineOptions::single_threaded(),
    );

    let unfixed = matches!(
        &outcome,
        Ok(Err(error)) if error.to_string().contains("failed at run time")
    );
    assert!(
        unfixed,
        "apache/datafusion#26745 no longer reproduces: remove this test and check its box on \
         issue #325. Outcome: {outcome:?}"
    );
}

/// A node on the path beneath an assertion that claims an order but keeps none of its input's
/// fails the assertion closed, though no sort appears: the check cannot tell where its order
/// came from.
#[test]
fn a_node_that_keeps_no_inputs_order_fails_the_assertion_closed() {
    let ctx = SessionContext::new_with_config(ordering_assertion::verify_order(
        pipeline::session_config(),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let sorted = MemTable::try_new(schema, vec![vec![batch]])
        .unwrap()
        .with_sort_order(vec![vec![col("x").sort(true, false)]]);
    let frame = ctx
        .read_table(Arc::new(KeepingNoOrder(Arc::new(sorted))))
        .unwrap();

    let failure = assertion_failure(frame, &[ascending("x")]);

    assert!(failure.contains("fails closed"), "{failure}");
    assert!(failure.contains("KeepingNoOrderExec"), "{failure}");
}

/// A table whose scan is its inner table's beneath a [`KeepingNoOrderExec`].
#[derive(Debug)]
struct KeepingNoOrder(Arc<dyn TableProvider>);

#[async_trait::async_trait]
impl TableProvider for KeepingNoOrder {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = self.0.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(KeepingNoOrderExec {
            cache: Arc::clone(input.properties()),
            input,
        }))
    }
}

/// An operator passing its input on that claims its input's properties, its ordering among them,
/// but reports keeping none of its input's order.
#[derive(Debug)]
struct KeepingNoOrderExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl DisplayAs for KeepingNoOrderExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeepingNoOrderExec")
    }
}

impl ExecutionPlan for KeepingNoOrderExec {
    fn name(&self) -> &'static str {
        "KeepingNoOrderExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Internal(
                "KeepingNoOrderExec takes one child".to_string(),
            ));
        };
        Ok(Arc::new(Self {
            cache: Arc::clone(input.properties()),
            input: Arc::clone(input),
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
        self.input.execute(partition, context)
    }
}
