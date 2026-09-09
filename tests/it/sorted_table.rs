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
        physical_plan::FileScanConfig,
        source::DataSourceExec,
    },
    execution::object_store::ObjectStoreUrl,
    physical_plan::{ExecutionPlanProperties, Partitioning},
    prelude::{SessionConfig, SessionContext, col},
};
use datafusion_sandbox::{
    locus::LocusRepresentation,
    pipeline::{self, PipelineOptions},
    sorted_table::{AttachedScalar, SortedTable},
};
use futures::TryStreamExt;
use object_store::path::Path;
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
    let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
    let scan_config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
    scan_config.file_groups[0]
        .files()
        .iter()
        .map(|file| file.object_meta.location.to_string())
        .collect()
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

    let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
    let scan_config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
    let files = scan_config.file_groups[0].files();
    assert_eq!(files[0].object_meta.location.as_ref(), "zzz.parquet");
    assert_eq!(files[1].object_meta.location.as_ref(), "aaa.parquet");
    assert_eq!(
        files[0].partition_values,
        vec![ScalarValue::Utf8(Some("sample-1".to_string()))]
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
        let table = first_sample_table(&ctx, fixture).await;
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
                let table = first_sample_table(&ctx, &fixture).await;
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
async fn first_sample_table(ctx: &SessionContext, fixture: &DatasetFixture) -> SortedTable {
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
    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let metas: Vec<_> = files.iter().map(|file| file.object_meta.clone()).collect();
    let schema = format
        .infer_schema(&ctx.state(), fixture.store(), &metas)
        .await
        .unwrap();
    SortedTable::new(
        fixture.table_path().object_store(),
        format,
        files,
        schema,
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
            col("alleles").sort(true, false),
        ],
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
