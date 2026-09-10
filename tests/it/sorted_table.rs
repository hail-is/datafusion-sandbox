mod format_contract;

use crate::fixture::{self, DatasetFixture, FixtureFormat, block_on};

use datafusion::{
    arrow::{
        array::{Array, Int32Array, StringArray},
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::{ColumnStatistics, DataFusionError, ScalarValue, Statistics, stats::Precision},
    datasource::{
        file_format::{FileFormat, parquet::ParquetFormat},
        listing::PartitionedFile,
        source::DataSourceExec,
    },
    execution::object_store::ObjectStoreUrl,
    physical_expr::{
        LexOrdering, PhysicalSortExpr, expressions::Column, projection::ProjectionExprs,
    },
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning, SortOrderPushdownResult, displayable,
    },
    prelude::{SessionConfig, SessionContext, col, lit},
};
use datafusion_sandbox::{
    locus::LocusRepresentation,
    pipeline::{self, PipelineOptions},
    sorted_table::{AttachedScalar, SortedTable},
};
use futures::TryStreamExt;
use object_store::path::Path;
use std::sync::Arc;
use vortex::VortexSessionDefault;

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

fn multi_column_table(files: Vec<PartitionedFile>) -> SortedTable {
    SortedTable::new(
        ObjectStoreUrl::local_filesystem(),
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

async fn file_group_paths(table: SortedTable) -> Vec<String> {
    file_group_paths_in(&SessionContext::new(), table).await
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

fn table(files: Vec<PartitionedFile>) -> SortedTable {
    SortedTable::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetFormat::default()),
        files,
        schema(),
        ordering(),
        None,
    )
}

#[tokio::test]
async fn scan_orders_files_and_stays_one_partition_under_a_hostile_session() {
    let mut config = SessionConfig::new().with_target_partitions(8);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let ctx = SessionContext::new_with_config(config);
    let table = SortedTable::new(
        ObjectStoreUrl::local_filesystem(),
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
    let paths = file_group_paths(multi_column_table(vec![
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
    ]))
    .await;

    assert_eq!(paths, ["first.parquet", "next.parquet"]);
}

/// A Hail-style cut between the alleles of one locus: the earlier file is constant on the
/// leading column and the later file extends past it, so their composed bounds overlap.
#[tokio::test]
async fn a_split_inside_a_leading_value_is_ordered_by_the_trailing_column() {
    let paths = file_group_paths(multi_column_table(vec![
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
    ]))
    .await;

    assert_eq!(paths, ["head.parquet", "tail.parquet"]);
}

#[tokio::test]
async fn a_cut_inside_a_locus_retains_ordering_without_a_sort() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let table = cut_inside_a_locus_table();
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

fn cut_inside_a_locus_table() -> SortedTable {
    multi_column_table(vec![
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
    ])
}

#[tokio::test]
async fn sort_pushdown_is_exact_for_the_declared_order_and_its_prefix() {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table()))
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
    let plan = ctx
        .read_table(Arc::new(table(vec![
            file("later.parquet", Some(2), Some(3)),
            file("earlier.parquet", Some(1), Some(2)),
        ])))
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
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table()))
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
    let plan = ctx
        .read_table(Arc::new(cut_inside_a_locus_table()))
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
                        let table = first_sample_table(&ctx, &fixture, format).await;
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
async fn zero_row_files_are_dropped_from_the_file_group() {
    let paths = file_group_paths(table(vec![
        file_with_rows("empty.parquet", 0, vec![column_statistics(None, None)]),
        file("rows.parquet", Some(1), Some(10)),
    ]))
    .await;

    assert_eq!(paths, ["rows.parquet"]);
}

#[tokio::test]
async fn overlapping_files_are_rejected_naming_both_paths() {
    let error = SessionContext::new()
        .read_table(Arc::new(table(vec![
            file("first.parquet", Some(1), Some(10)),
            file("overlap.parquet", Some(5), Some(15)),
        ])))
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
    let error = SessionContext::new()
        .read_table(Arc::new(table(vec![file("missing.parquet", None, None)])))
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
        let table = first_sample_table(&ctx, fixture, FixtureFormat::Parquet).await;
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
                let table = first_sample_table(&ctx, &fixture, FixtureFormat::Parquet).await;
                ctx.read_table(Arc::new(table))?.collect().await
            }
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    let loci: Vec<(String, i32)> = batches.iter().flat_map(locus_column_values).collect();
    assert_eq!(loci.len(), 8, "{loci:?}");
    assert!(
        loci.windows(2).all(|pair| pair[0] <= pair[1]),
        "rows out of locus order: {loci:?}"
    );
}

fn locus_column_values(batch: &RecordBatch) -> Vec<(String, i32)> {
    // Parquet reads strings as `Utf8View` by default; cast so one array type covers both formats.
    let contigs = cast(batch.column_by_name("contig").unwrap(), &DataType::Utf8).unwrap();
    let contigs = contigs.as_any().downcast_ref::<StringArray>().unwrap();
    let positions = batch
        .column_by_name("position")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (contigs.value(row).to_string(), positions.value(row)))
        .collect()
}

/// Lists the first sample's files from the fixture store with no statistics, so the table has
/// to infer them from the file footers.
async fn first_sample_table(
    ctx: &SessionContext,
    fixture: &DatasetFixture,
    format: FixtureFormat,
) -> SortedTable {
    let sample = fixture::SAMPLES[0];
    let prefix = Path::from(format!("{}/s={sample}", fixture.table_path().prefix()));
    let files: Vec<PartitionedFile> = fixture
        .store()
        .list(Some(&prefix))
        .map_ok(PartitionedFile::new_from_meta)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(files.len(), 4);
    let format: Arc<dyn FileFormat> = match format {
        FixtureFormat::Parquet => Arc::new(ParquetFormat::default()),
        FixtureFormat::Vortex => Arc::new(vortex_datafusion::VortexFormat::new(
            vortex::session::VortexSession::default(),
        )),
    };
    let metas: Vec<_> = files.iter().map(|file| file.object_meta.clone()).collect();
    let schema = format
        .infer_schema(&ctx.state(), fixture.store(), &metas)
        .await
        .unwrap();
    let ordering = if schema.index_of("locus").is_ok() {
        vec![
            col("locus").sort(true, false),
            col("alleles").sort(true, false),
        ]
    } else {
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
            col("alleles").sort(true, false),
        ]
    };
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
    let table = SortedTable::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetFormat::default()),
        vec![file("only.parquet", Some(1), Some(10))],
        schema(),
        vec![col("source").sort(true, false)],
        Some(AttachedScalar {
            field: Arc::new(Field::new("source", DataType::Utf8, false)),
            value: ScalarValue::Utf8(Some("sample-1".to_string())),
        }),
    );

    let error = SessionContext::new()
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("source"), "{error}");
}
