mod filtered_scans;
mod format_contract;
mod metadata_collection;

use crate::fixture::{self, DatasetFixture, FixtureFormat, MemoryStore, block_on};

use crate::{
    locus::{LocusOrdering, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    sorted_table::{AttachedScalar, SortedTable},
};
use datafusion::{
    arrow::{
        array::{Array, StringArray},
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    catalog::TableProvider,
    common::{ColumnStatistics, DataFusionError, ScalarValue, Statistics, stats::Precision},
    datasource::{
        file_format::{FileFormat, parquet::ParquetFormat},
        listing::PartitionedFile,
        source::DataSourceExec,
    },
    logical_expr::{Expr, TableProviderFilterPushDown, expr_fn::unnest},
    physical_expr::{
        LexOrdering, PhysicalSortExpr, expressions::Column, projection::ProjectionExprs,
    },
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning, SortOrderPushdownResult,
        StatisticsArgs, StatisticsContext, displayable,
    },
    prelude::{SessionConfig, SessionContext, col, lit},
};
use std::sync::Arc;

fn column_statistics(min: Option<i32>, max: Option<i32>) -> ColumnStatistics {
    ColumnStatistics {
        null_count: Precision::Exact(0),
        min_value: min
            .map(|value| ScalarValue::Int32(Some(value)))
            .map_or(Precision::Absent, Precision::Exact),
        max_value: max
            .map(|value| ScalarValue::Int32(Some(value)))
            .map_or(Precision::Absent, Precision::Exact),
        ..Default::default()
    }
}

fn file_with_statistics(path: &str, columns: Vec<ColumnStatistics>) -> PartitionedFile {
    file_with_rows(path, 1, columns)
}

fn file_with_rows(path: &str, num_rows: usize, columns: Vec<ColumnStatistics>) -> PartitionedFile {
    PartitionedFile::new(path, 1).with_statistics(Arc::new(Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Absent,
        column_statistics: columns,
    }))
}

/// A file with a known byte size and row-count precision, and `position` in `min..=max`.
fn sized_file(
    path: &str,
    num_rows: Precision<usize>,
    bytes: usize,
    min: i32,
    max: i32,
) -> PartitionedFile {
    PartitionedFile::new(path, 1).with_statistics(Arc::new(Statistics {
        num_rows,
        total_byte_size: Precision::Exact(bytes),
        column_statistics: vec![column_statistics(Some(min), Some(max))],
    }))
}

/// The statistics `plan` reports for one partition, or for all of them.
fn statistics(plan: &dyn ExecutionPlan, partition: Option<usize>) -> Arc<Statistics> {
    StatisticsContext::new()
        .compute(plan, &StatisticsArgs::new().with_partition(partition))
        .unwrap()
}

fn multi_column_table(store: &MemoryStore, files: Vec<PartitionedFile>) -> SortedTable {
    SortedTable::new(
        store.url().clone(),
        Arc::new(ParquetFormat::default()),
        files,
        Arc::new(Schema::new(vec![
            Field::new("major", DataType::Int32, false),
            Field::new("minor", DataType::Int32, false),
        ])),
        vec![
            col("major").sort(true, false),
            col("minor").sort(true, false),
        ],
        None,
    )
}

/// Plans `table` on a fresh session that knows `store`, the store the table was built over.
async fn file_group_paths(store: &MemoryStore, table: SortedTable) -> Vec<String> {
    let ctx = SessionContext::new();
    store.register(&ctx);
    file_group_paths_in(&ctx, table).await
}

async fn file_group_paths_in(ctx: &SessionContext, table: SortedTable) -> Vec<String> {
    let plan = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    displayed_file_paths(plan.as_ref())
}

// EXPLAIN is a public observation of the file group, independent of the source's type.
fn displayed_file_paths(plan: &dyn ExecutionPlan) -> Vec<String> {
    let text = displayable(plan).indent(true).to_string();
    let (_, group) = text
        .split_once("file_groups={1 group: [[")
        .unwrap_or_else(|| panic!("{text}"));
    let (paths, _) = group.split_once("]]").unwrap();
    paths.split(", ").map(ToString::to_string).collect()
}

fn file(path: &str, min: Option<i32>, max: Option<i32>) -> PartitionedFile {
    file_with_statistics(path, vec![column_statistics(min, max)])
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        false,
    )]))
}

fn ordering() -> Vec<datafusion::logical_expr::SortExpr> {
    vec![col("position").sort(true, false)]
}

fn table(store: &MemoryStore, files: Vec<PartitionedFile>) -> SortedTable {
    SortedTable::new(
        store.url().clone(),
        Arc::new(ParquetFormat::default()),
        files,
        schema(),
        ordering(),
        None,
    )
}

fn scalar_table(
    store: &MemoryStore,
    files: Vec<PartitionedFile>,
    value: ScalarValue,
) -> SortedTable {
    SortedTable::new(
        store.url().clone(),
        Arc::new(ParquetFormat::default()),
        files,
        schema(),
        ordering(),
        Some(AttachedScalar {
            field: Arc::new(Field::new("source", DataType::Utf8, true)),
            value,
        }),
    )
}

#[tokio::test]
#[expect(
    deprecated,
    reason = "wildcards are explicitly excluded from exact filters"
)]
async fn attached_scalar_filter_classification_matches_the_expression_contract() {
    use datafusion::logical_expr::expr_fn::{exists, in_subquery, scalar_subquery};
    let store = MemoryStore::new("scalar-filters");
    let table = Arc::new(scalar_table(&store, vec![], ScalarValue::from("sample-1")));
    let ctx = SessionContext::new();
    ctx.register_table("t", table.clone()).unwrap();
    let df_schema = table.schema().try_into().unwrap();
    let subquery = Arc::new(
        ctx.sql("SELECT CAST(position AS VARCHAR) FROM t")
            .await
            .unwrap()
            .into_unoptimized_plan(),
    );
    let parse = |sql| ctx.parse_sql_expr(sql, &df_schema).unwrap();
    let exact = vec![
        col("source").eq(lit("sample-1")),
        parse("source IN ('sample-1', 'sample-2') AND source IS NOT NULL"),
        parse("lower(source) = 'sample-1'"),
        parse("source = CAST(current_date() AS VARCHAR)"),
        parse("source = NULL"),
        lit(true),
    ];
    let inexact = vec![
        col("position").gt(lit(1)),
        col("source")
            .eq(lit("sample-1"))
            .and(col("position").gt(lit(1))),
        col("source")
            .eq(lit("sample-1"))
            .or(col("position").gt(lit(1))),
        parse("source = CAST(random() AS VARCHAR)"),
        parse("max(source) = 'sample-1'"),
        parse("first_value(source) OVER () = 'sample-1'"),
        Expr::Wildcard {
            qualifier: None,
            options: Box::default(),
        },
        unnest(col("source")),
        parse("source = $1"),
        col("source").eq(scalar_subquery(Arc::clone(&subquery))),
        in_subquery(col("source"), Arc::clone(&subquery)),
        exists(subquery),
        Expr::OuterReferenceColumn(
            Arc::new(Field::new("position", DataType::Int32, false)),
            datafusion::common::Column::new_unqualified("position"),
        ),
        Expr::ScalarVariable(
            Arc::new(Field::new("external", DataType::Utf8, true)),
            vec!["external".to_string()],
        ),
    ];
    for (filters, expected) in [
        (&exact, TableProviderFilterPushDown::Exact),
        (&inexact, TableProviderFilterPushDown::Inexact),
    ] {
        let refs: Vec<_> = filters.iter().collect();
        let classifications = table.supports_filters_pushdown(&refs).unwrap();
        assert_eq!(
            classifications,
            vec![expected; filters.len()],
            "{filters:?}"
        );
    }
    let without_scalar = self::table(&store, vec![]);
    assert_eq!(
        without_scalar
            .supports_filters_pushdown(&[&lit(true), &col("position").gt(lit(1))])
            .unwrap(),
        vec![TableProviderFilterPushDown::Inexact; 2],
    );
}

#[test]
fn false_and_null_scalar_filters_return_projected_empty_plans_without_opening_files() {
    pipeline::run(
        |ctx| async move {
            let store = MemoryStore::new("scalar-filters");
            store.register(&ctx);
            // No file exists: either footer inference or execution-time opens would fail.
            let table = scalar_table(
                &store,
                vec![PartitionedFile::new("missing.parquet", 100)],
                ScalarValue::from("sample-1"),
            );
            for filter in [
                col("source").eq(lit("sample-2")),
                col("source").eq(lit(ScalarValue::Utf8(None))),
                lit(ScalarValue::Null),
                lit(ScalarValue::Boolean(None)),
            ] {
                for projection in [None, Some(vec![0]), Some(vec![1, 0]), Some(vec![])] {
                    let plan = table
                        .scan(
                            &ctx.state(),
                            projection.as_ref(),
                            std::slice::from_ref(&filter),
                            None,
                        )
                        .await?;
                    assert!(plan.is::<datafusion::physical_plan::empty::EmptyExec>());
                    let expected = projection.as_ref().map_or_else(
                        || table.schema(),
                        |indices| Arc::new(table.schema().project(indices).unwrap()),
                    );
                    assert_eq!(plan.schema(), expected);
                    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
                    assert_eq!(batches, []);
                }
            }
            // The same scan without exclusion must attempt to read the missing footer.
            let error = table.scan(&ctx.state(), None, &[], None).await.unwrap_err();
            assert!(error.to_string().contains("missing.parquet"), "{error}");
            Ok(())
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();
}

#[tokio::test]
async fn true_scalar_filters_leave_the_scan_unchanged_with_its_limit() {
    let ctx = SessionContext::new();
    let store = MemoryStore::new("scalar-filters");
    store.register(&ctx);
    let table = Arc::new(scalar_table(
        &store,
        vec![
            file("later.parquet", Some(11), Some(20)),
            file("earlier.parquet", Some(1), Some(10)),
        ],
        ScalarValue::from("sample-1"),
    ));
    let df_schema = table.schema().try_into().unwrap();
    let filters = [
        col("source").eq(lit("sample-1")),
        ctx.parse_sql_expr(
            "lower(source) = 'sample-1' AND current_date() = current_date()",
            &df_schema,
        )
        .unwrap(),
    ];
    let projection = vec![0];
    let baseline = table
        .scan(&ctx.state(), Some(&projection), &[], Some(9))
        .await
        .unwrap();
    let plan = table
        .scan(&ctx.state(), Some(&projection), &filters, Some(9))
        .await
        .unwrap();
    assert_eq!(plan.fetch(), Some(9));
    assert_eq!(plan.schema(), baseline.schema());
    assert_eq!(plan.output_ordering(), baseline.output_ordering());
    assert_eq!(
        displayable(plan.as_ref()).indent(true).to_string(),
        displayable(baseline.as_ref()).indent(true).to_string()
    );

    let inexact = table
        .scan(&ctx.state(), None, &[col("position").gt(lit(5))], Some(9))
        .await
        .unwrap();
    assert_eq!(inexact.fetch(), Some(9));
    let plan = ctx
        .read_table(table)
        .unwrap()
        .filter(filters[0].clone())
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(
        plan.is::<DataSourceExec>(),
        "{}",
        displayable(plan.as_ref()).indent(true)
    );
}

#[test]
fn inexact_filters_keep_the_logical_residual_and_do_not_truncate_before_filtering() {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::LogicalPlan;

    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for decode_time_filtering in [false, true] {
            let fixture = Arc::clone(fixture::dataset_fixture(
                format,
                LocusRepresentation::ContigPosition,
            ));
            pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        ctx.state_ref()
                            .write()
                            .config_mut()
                            .options_mut()
                            .execution
                            .parquet
                            .pushdown_filters = decode_time_filtering;
                        let table = sample_table(&ctx, &fixture, fixture::SAMPLES[0]).await;
                        let df = ctx
                            .read_table(Arc::new(table))?
                            .filter(col("position").gt_eq(lit(3)))?
                            .limit(0, Some(2))?;
                        let logical = df.clone().into_optimized_plan()?;
                        let mut residuals = 0;
                        let mut scans = 0;
                        logical.apply(|node| {
                            match node {
                                LogicalPlan::Filter(_) => residuals += 1,
                                LogicalPlan::TableScan(scan) => {
                                    scans += 1;
                                    assert_eq!(scan.filters.len(), 1);
                                    assert_eq!(
                                        scan.fetch, None,
                                        "limit must stay above the inexact filter"
                                    );
                                }
                                _ => {}
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                        assert_eq!((residuals, scans), (1, 1));
                        let plan = df.create_physical_plan().await?;
                        let text = displayable(plan.as_ref()).indent(true).to_string();
                        assert!(
                            text.contains("predicate=") || text.contains("predicate:"),
                            "format-level pushdown must retain the predicate: {text}"
                        );
                        if matches!(format, FixtureFormat::Parquet) && !decode_time_filtering {
                            assert!(text.contains("FilterExec"), "{text}");
                        } else {
                            assert!(!text.contains("FilterExec"), "{text}");
                        }
                        let batches =
                            datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
                        let loci: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| {
                                fixture::decode_loci(batch, LocusRepresentation::ContigPosition)
                            })
                            .collect();
                        // Without ORDER BY, the residual filter's repartition may change
                        // which matching rows satisfy the limit.
                        assert_eq!(loci.len(), 2, "{loci:?}");
                        assert!(
                            loci.iter().all(|(contig, position)| matches!(
                                (contig.as_str(), position),
                                ("chr1", 3 | 4) | ("chr2", 3)
                            )),
                            "{loci:?}"
                        );
                        Ok(())
                    }
                },
                PipelineOptions {
                    threads: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        }
    }
}

#[tokio::test]
async fn scan_orders_files_and_stays_one_partition_under_a_hostile_session() {
    let mut config = SessionConfig::new().with_target_partitions(8);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let ctx = SessionContext::new_with_config(config);
    let store = MemoryStore::new("hostile-session");
    store.register(&ctx);
    let table = SortedTable::new(
        store.url().clone(),
        Arc::new(ParquetFormat::default()),
        vec![
            file("aaa.parquet", Some(11), Some(20)),
            file("zzz.parquet", Some(1), Some(10)),
        ],
        schema(),
        ordering(),
        Some(AttachedScalar {
            field: Arc::new(Field::new("source", DataType::Utf8, false)),
            value: ScalarValue::Utf8(Some("sample-1".to_string())),
        }),
    );

    let plan = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    assert!(matches!(
        plan.output_partitioning(),
        Partitioning::UnknownPartitioning(1)
    ));
    assert!(plan.output_ordering().is_some());
    assert!(
        plan.repartitioned(8, ctx.state().config_options())
            .unwrap()
            .is_none()
    );

    assert_eq!(
        displayed_file_paths(plan.as_ref()),
        ["zzz.parquet", "aaa.parquet"]
    );
    assert_eq!(
        plan.schema().field_with_name("source").unwrap().data_type(),
        &DataType::Utf8
    );
}

#[tokio::test]
async fn multi_column_ranges_are_compared_lexicographically() {
    let store = MemoryStore::new("multi-column");
    let paths = file_group_paths(
        &store,
        multi_column_table(
            &store,
            vec![
                file_with_statistics(
                    "next.parquet",
                    vec![
                        column_statistics(Some(1), Some(1)),
                        column_statistics(Some(4), Some(6)),
                    ],
                ),
                file_with_statistics(
                    "first.parquet",
                    vec![
                        column_statistics(Some(1), Some(1)),
                        column_statistics(Some(1), Some(3)),
                    ],
                ),
            ],
        ),
    )
    .await;

    assert_eq!(paths, ["first.parquet", "next.parquet"]);
}

/// A Hail-style cut between the alleles of one locus: the earlier file is constant on the
/// leading column and the later file extends past it, so their composed bounds overlap.
#[tokio::test]
async fn a_split_inside_a_leading_value_is_ordered_by_the_trailing_column() {
    let store = MemoryStore::new("split-inside-a-leading-value");
    let paths = file_group_paths(
        &store,
        multi_column_table(
            &store,
            vec![
                file_with_statistics(
                    "tail.parquet",
                    vec![
                        column_statistics(Some(1), Some(2)),
                        column_statistics(Some(1), Some(5)),
                    ],
                ),
                file_with_statistics(
                    "head.parquet",
                    vec![
                        column_statistics(Some(1), Some(1)),
                        column_statistics(Some(1), Some(3)),
                    ],
                ),
            ],
        ),
    )
    .await;

    assert_eq!(paths, ["head.parquet", "tail.parquet"]);
}

#[tokio::test]
async fn a_cut_inside_a_locus_retains_ordering_without_a_sort() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let store = MemoryStore::new("cut-inside-a-locus");
    store.register(&ctx);
    let table = cut_inside_a_locus_table(&store);
    let df = ctx.read_table(Arc::new(table)).unwrap();
    let scan = df.clone().create_physical_plan().await.unwrap();
    assert!(scan.output_ordering().is_some(), "{scan:?}");
    let plan = df
        .sort(vec![
            col("major").sort(true, false),
            col("minor").sort(true, false),
        ])
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(plan.is::<DataSourceExec>(), "{plan:?}");
}

fn cut_inside_a_locus_table(store: &MemoryStore) -> SortedTable {
    multi_column_table(
        store,
        vec![
            file_with_statistics(
                "tail.parquet",
                vec![
                    column_statistics(Some(1), Some(2)),
                    column_statistics(Some(1), Some(5)),
                ],
            ),
            file_with_statistics(
                "head.parquet",
                vec![
                    column_statistics(Some(1), Some(1)),
                    column_statistics(Some(1), Some(3)),
                ],
            ),
        ],
    )
}

#[tokio::test]
async fn sort_pushdown_is_exact_for_the_declared_order_and_its_prefix() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let store = MemoryStore::new("sort-pushdown");
    store.register(&ctx);
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table(&store)))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let order = plan.output_ordering().unwrap();
    for request in [order.to_vec(), vec![order[0].clone()]] {
        let SortOrderPushdownResult::Exact { inner } = plan.try_pushdown_sort(&request).unwrap()
        else {
            panic!("expected exact sort pushdown for {request:?}");
        };
        assert_eq!(inner.output_ordering(), Some(order));
        assert_eq!(
            displayed_file_paths(inner.as_ref()),
            ["head.parquet", "tail.parquet"]
        );
    }
    let other = vec![PhysicalSortExpr {
        expr: Arc::new(Column::new("minor", 1)),
        options: order[0].options,
    }];
    assert!(!matches!(
        plan.try_pushdown_sort(&other).unwrap(),
        SortOrderPushdownResult::Exact { .. }
    ));
}

#[tokio::test]
async fn touching_bounds_accept_two_files_sharing_a_locus_under_locus_only_ordering() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let store = MemoryStore::new("touching-bounds");
    store.register(&ctx);
    let plan = ctx
        .read_table(Arc::new(table(
            &store,
            vec![
                file("later.parquet", Some(2), Some(3)),
                file("earlier.parquet", Some(1), Some(2)),
            ],
        )))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(plan.output_ordering().is_some());
    assert_eq!(
        displayed_file_paths(plan.as_ref()),
        ["earlier.parquet", "later.parquet"]
    );
}

#[tokio::test]
async fn fetch_and_projection_swaps_preserve_only_the_projected_ordering() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let store = MemoryStore::new("fetch-and-projection");
    store.register(&ctx);
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table(&store)))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let fetched = plan.with_fetch(Some(7)).unwrap();
    assert_eq!(fetched.fetch(), Some(7));
    assert_eq!(fetched.output_ordering(), plan.output_ordering());
    let scan = fetched.downcast_ref::<DataSourceExec>().unwrap();
    let projection = ProjectionExprs::from_indices(&[1, 0], &scan.schema());
    let source = scan
        .data_source()
        .try_swapping_with_projection(&projection)
        .unwrap()
        .unwrap();
    let swapped = DataSourceExec::new(source);
    let options = plan.output_ordering().unwrap()[0].options;
    let expected = LexOrdering::new(vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("major", 1)),
            options,
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("minor", 0)),
            options,
        },
    ])
    .unwrap();
    assert_eq!(swapped.properties().output_ordering(), Some(&expected));
    assert_eq!(swapped.fetch(), Some(7));
    assert!(
        swapped
            .repartitioned(8, ctx.state().config_options())
            .unwrap()
            .is_none()
    );
    for (indices, expected) in [(vec![1], Some("major@0 ASC NULLS LAST")), (vec![0], None)] {
        let projection = ProjectionExprs::from_indices(&indices, &swapped.schema());
        let source = swapped
            .data_source()
            .try_swapping_with_projection(&projection)
            .unwrap()
            .unwrap();
        let projected = DataSourceExec::new(source);
        assert_eq!(
            projected
                .properties()
                .output_ordering()
                .map(ToString::to_string)
                .as_deref(),
            expected
        );
        assert!(matches!(
            projected.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(1)
        ));
    }
}

#[tokio::test]
async fn projection_uses_filter_equivalences_to_preserve_the_remaining_ordering() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let store = MemoryStore::new("filter-equivalences");
    store.register(&ctx);
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table(&store)))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
    let predicate = ctx
        .create_physical_expr(col("major").eq(lit(1)), &scan.schema().try_into().unwrap())
        .unwrap();
    let mut options = ctx.state().config_options().as_ref().clone();
    options.execution.parquet.pushdown_filters = true;
    let source = scan
        .data_source()
        .try_pushdown_filters(vec![predicate], &options)
        .unwrap()
        .updated_node
        .unwrap();
    let projection = ProjectionExprs::from_indices(&[1], &scan.schema());
    let source = source
        .try_swapping_with_projection(&projection)
        .unwrap()
        .unwrap();
    let projected: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(source));
    let expected = LexOrdering::new(vec![PhysicalSortExpr {
        expr: Arc::new(Column::new("minor", 0)),
        options: plan.output_ordering().unwrap()[1].options,
    }])
    .unwrap();
    assert_eq!(projected.output_ordering(), Some(&expected));
    assert!(matches!(
        projected.try_pushdown_sort(&expected).unwrap(),
        SortOrderPushdownResult::Exact { .. }
    ));
}

#[test]
fn physical_filter_pushdown_after_projection_keeps_one_ordered_partition_in_both_formats() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = Arc::clone(fixture::dataset_fixture(format, representation));
            let batches = pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let table = sample_table(&ctx, &fixture, fixture::SAMPLES[0]).await;
                        let plan = ctx
                            .read_table(Arc::new(table))?
                            .create_physical_plan()
                            .await?;
                        let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
                        let indices: Vec<_> = (0..plan.schema().fields().len()).rev().collect();
                        let projection = ProjectionExprs::from_indices(&indices, &plan.schema());
                        let source = scan
                            .data_source()
                            .try_swapping_with_projection(&projection)?
                            .unwrap();
                        let projected = DataSourceExec::new(source);
                        let expected = projected.properties().output_ordering().unwrap().clone();
                        let predicate = ctx.create_physical_expr(
                            col("alleles").gt(lit("A,C")),
                            &projected.schema().try_into()?,
                        )?;
                        // Enable decode-time filtering for this test, not the shared session.
                        let mut options = ctx.state().config_options().as_ref().clone();
                        options.execution.parquet.pushdown_filters = true;
                        options.optimizer.repartition_file_min_size = 0;
                        let pushed = projected
                            .data_source()
                            .try_pushdown_filters(vec![predicate], &options)?;
                        assert!(pushed.filters.iter().all(|filter| matches!(
                            filter,
                            datafusion::physical_plan::filter_pushdown::PushedDown::Yes
                        )));
                        let source = pushed
                            .updated_node
                            .expect("format must rebuild its source for the filter");
                        let source = source.with_fetch(Some(3)).unwrap();
                        let filtered: Arc<dyn ExecutionPlan> =
                            Arc::new(DataSourceExec::new(source));
                        assert_eq!(filtered.output_ordering(), Some(&expected));
                        assert!(matches!(
                            filtered.output_partitioning(),
                            Partitioning::UnknownPartitioning(1)
                        ));
                        assert!(filtered.repartitioned(8, &options)?.is_none());
                        datafusion::physical_plan::collect(filtered, ctx.task_ctx()).await
                    }
                },
                PipelineOptions {
                    threads: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            for batch in batches {
                let alleles =
                    cast(batch.column_by_name("alleles").unwrap(), &DataType::Utf8).unwrap();
                let alleles = alleles.as_any().downcast_ref::<StringArray>().unwrap();
                assert!(alleles.iter().all(|value| value == Some("A,G")));
            }
        }
    }
}

#[tokio::test]
async fn scan_statistics_aggregate_the_files_in_the_scan() {
    let ctx = SessionContext::new();
    let store = MemoryStore::new("aggregate-statistics");
    store.register(&ctx);
    let files = || {
        vec![
            sized_file("later.parquet", Precision::Exact(5), 50, 11, 20),
            file_with_rows("empty.parquet", 0, vec![column_statistics(None, None)]),
            sized_file("earlier.parquet", Precision::Exact(3), 30, 1, 10),
        ]
    };
    let value = ScalarValue::from("sample-1");
    let table = scalar_table(&store, files(), value.clone());
    let position = ColumnStatistics {
        null_count: Precision::Exact(0),
        min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
        max_value: Precision::Exact(ScalarValue::Int32(Some(20))),
        ..Default::default()
    };

    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let statistics = self::statistics(plan.as_ref(), None);
    assert_eq!(statistics.num_rows, Precision::Exact(8));
    assert_eq!(statistics.column_statistics.len(), 2);
    assert_eq!(
        statistics.column_statistics[0].min_value,
        position.min_value
    );
    assert_eq!(
        statistics.column_statistics[0].max_value,
        position.max_value
    );
    assert_eq!(
        statistics.column_statistics[0].null_count,
        position.null_count
    );
    let source = &statistics.column_statistics[1];
    assert_eq!(source.min_value, Precision::Exact(value.clone()));
    assert_eq!(source.max_value, Precision::Exact(value.clone()));
    assert_eq!(source.null_count, Precision::Exact(0));
    assert_eq!(self::statistics(plan.as_ref(), Some(0)), statistics);

    // Projection keeps the statistics aligned with the projected schema.
    let projected = table
        .scan(&ctx.state(), Some(&vec![1]), &[], None)
        .await
        .unwrap();
    let statistics = self::statistics(projected.as_ref(), None);
    assert_eq!(statistics.num_rows, Precision::Exact(8));
    assert_eq!(statistics.column_statistics.len(), 1);
    assert_eq!(
        statistics.column_statistics[0].min_value,
        Precision::Exact(value)
    );

    // An inexact input stays inexact rather than being promoted to exact.
    let mut inexact = files();
    inexact[0] = sized_file("later.parquet", Precision::Inexact(5), 50, 11, 20);
    let table = self::table(&store, inexact);
    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let statistics = self::statistics(plan.as_ref(), None);
    assert_eq!(statistics.num_rows, Precision::Inexact(8));
    assert_eq!(statistics.column_statistics.len(), 1);
    assert_eq!(
        statistics.column_statistics[0].max_value,
        position.max_value
    );
}

#[tokio::test]
async fn zero_row_files_are_dropped_from_the_file_group() {
    let store = MemoryStore::new("zero-row-files");
    let paths = file_group_paths(
        &store,
        table(
            &store,
            vec![
                file_with_rows("empty.parquet", 0, vec![column_statistics(None, None)]),
                file("rows.parquet", Some(1), Some(10)),
            ],
        ),
    )
    .await;

    assert_eq!(paths, ["rows.parquet"]);
}

#[tokio::test]
async fn overlapping_files_are_rejected_naming_both_paths() {
    let store = MemoryStore::new("overlapping-files");
    let ctx = SessionContext::new();
    store.register(&ctx);
    let error = ctx
        .read_table(Arc::new(table(
            &store,
            vec![
                file("first.parquet", Some(1), Some(10)),
                file("overlap.parquet", Some(5), Some(15)),
            ],
        )))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("first.parquet"), "{error}");
    assert!(error.to_string().contains("overlap.parquet"), "{error}");
}

#[tokio::test]
async fn missing_statistics_are_rejected_naming_the_path_and_column() {
    let store = MemoryStore::new("missing-statistics");
    let ctx = SessionContext::new();
    store.register(&ctx);
    let error = ctx
        .read_table(Arc::new(table(
            &store,
            vec![file("missing.parquet", None, None)],
        )))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("missing.parquet"), "{error}");
    assert!(error.to_string().contains("position"), "{error}");
}

/// The fixture's file names sort against locus order, so a file group in locus order can only
/// have come from statistics the table inferred from the file footers.
#[test]
fn inferred_statistics_order_the_files_against_path_order() {
    let fixture =
        fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::ContigPosition);

    let paths = block_on(async {
        let ctx = SessionContext::new();
        fixture.register(&ctx);
        let table = sample_table(&ctx, fixture, fixture::SAMPLES[0]).await;
        file_group_paths_in(&ctx, table).await
    });

    let names: Vec<_> = paths
        .iter()
        .map(|path| path.rsplit('/').next().unwrap())
        .collect();
    assert_eq!(names, ["d.parquet", "c.parquet", "b.parquet", "a.parquet"]);
}

/// The one place the repo checks the guarantee the sorted table exists for.
#[test]
fn collected_rows_arrive_in_locus_order() {
    let fixture = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
    ));

    let batches = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let table = sample_table(&ctx, &fixture, fixture::SAMPLES[0]).await;
                ctx.read_table(Arc::new(table))?.collect().await
            }
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    let loci: Vec<(String, i32)> = batches
        .iter()
        .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
        .collect();
    assert_eq!(loci.len(), 8, "{loci:?}");
    assert!(
        loci.windows(2).all(|pair| pair[0] <= pair[1]),
        "rows out of locus order: {loci:?}"
    );
}

/// Lists one sample's files from the fixture store with no statistics, so the table has to
/// infer them from the file footers. Schema inference mirrors dataset discovery: the fixture
/// cannot offer its written schema because Parquet reads `Utf8` back as `Utf8View`.
async fn sample_table(ctx: &SessionContext, fixture: &DatasetFixture, sample: &str) -> SortedTable {
    let format = fixture.input_format().read_format();
    sample_table_with_format(ctx, fixture, sample, format).await
}

/// `sample_table` reading through `format`, for tests that observe the reads.
async fn sample_table_with_format(
    ctx: &SessionContext,
    fixture: &DatasetFixture,
    sample: &str,
    format: Arc<dyn FileFormat>,
) -> SortedTable {
    let metas = fixture.sample_files(sample).await;
    let schema = format
        .infer_schema(&ctx.state(), fixture.store(), &metas)
        .await
        .unwrap();
    let ordering = LocusOrdering::locus_then_alleles()
        .expand(fixture.representation())
        .sort_expressions();
    let files = metas
        .into_iter()
        .map(PartitionedFile::new_from_meta)
        .collect();
    SortedTable::new(
        fixture.table_path().object_store(),
        format,
        files,
        schema,
        ordering,
        None,
    )
}

#[tokio::test]
async fn an_ordering_column_absent_from_the_file_schema_is_rejected() {
    let store = MemoryStore::new("absent-ordering-column");
    let table = SortedTable::new(
        store.url().clone(),
        Arc::new(ParquetFormat::default()),
        vec![file("only.parquet", Some(1), Some(10))],
        schema(),
        vec![col("source").sort(true, false)],
        Some(AttachedScalar {
            field: Arc::new(Field::new("source", DataType::Utf8, false)),
            value: ScalarValue::Utf8(Some("sample-1".to_string())),
        }),
    );

    let ctx = SessionContext::new();
    store.register(&ctx);
    let error = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("source"), "{error}");
}
