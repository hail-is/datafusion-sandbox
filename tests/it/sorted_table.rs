use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::{ColumnStatistics, DataFusionError, ScalarValue, Statistics, stats::Precision},
    datasource::{
        file_format::parquet::ParquetFormat, listing::PartitionedFile,
        physical_plan::FileScanConfig, source::DataSourceExec,
    },
    execution::object_store::ObjectStoreUrl,
    physical_plan::{ExecutionPlanProperties, Partitioning},
    prelude::{SessionConfig, SessionContext, col},
};
use datafusion_sandbox::sorted_table::{AttachedScalar, SortedTable};
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
    PartitionedFile::new(path, 1).with_statistics(Arc::new(Statistics {
        num_rows: Precision::Exact(1),
        total_byte_size: Precision::Absent,
        column_statistics: columns,
    }))
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
    let table = SortedTable::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetFormat::default()),
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
        Arc::new(Schema::new(vec![
            Field::new("major", DataType::Int32, false),
            Field::new("minor", DataType::Int32, false),
        ])),
        vec![
            col("major").sort(true, false),
            col("minor").sort(true, false),
        ],
        None,
    );

    let plan = SessionContext::new()
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
    let scan_config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
    let files = scan_config.file_groups[0].files();
    assert_eq!(files[0].object_meta.location.as_ref(), "first.parquet");
    assert_eq!(files[1].object_meta.location.as_ref(), "next.parquet");
}

#[tokio::test]
async fn overlapping_files_are_rejected_with_the_offending_path() {
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
    assert!(error.to_string().contains("overlap.parquet"), "{error}");
}

#[tokio::test]
async fn missing_statistics_are_rejected_with_the_offending_path() {
    let error = SessionContext::new()
        .read_table(Arc::new(table(vec![file("missing.parquet", None, None)])))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("missing.parquet"), "{error}");
}
