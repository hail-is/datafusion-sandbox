use super::{column_statistics, file_with_statistics};
use datafusion_sandbox::fixture::MemoryStore;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::{Constraint, Constraints, DataFusionError, Result, Statistics, stats::Precision},
    datasource::{
        MemTable,
        file_format::{
            FileFormat, file_compression_type::FileCompressionType, parquet::ParquetFormat,
        },
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    logical_expr::Operator,
    physical_expr::{
        LexOrdering,
        expressions::{BinaryExpr, Column, Literal},
        projection::ProjectionExprs,
    },
    physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning, empty::EmptyExec},
    prelude::{SessionContext, col},
};
use datafusion_sandbox::sorted_table::SortedTable;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum ReturnedPlan {
    RetainConfig,
    NotDataSourceExec,
    NotFileScanConfig,
    WithoutOrdering,
    WeakerOrdering,
    FilteredProjection,
    WithoutPartitioning,
    MultipleFileGroups,
}

#[derive(Debug)]
struct TestFormat {
    parquet: ParquetFormat,
    returned_plan: ReturnedPlan,
    calls: Mutex<Vec<FileScanConfig>>,
}

impl TestFormat {
    fn new(returned_plan: ReturnedPlan) -> Self {
        Self {
            parquet: ParquetFormat::default(),
            returned_plan,
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl FileFormat for TestFormat {
    fn get_ext(&self) -> String {
        "format-contract-test".to_string()
    }

    fn get_ext_with_compression(&self, compression: &FileCompressionType) -> Result<String> {
        self.parquet.get_ext_with_compression(compression)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.parquet.compression_type()
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        panic!("the caller supplies the schema")
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        panic!("the caller supplies file statistics")
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.calls.lock().unwrap().push(conf.clone());
        match self.returned_plan {
            ReturnedPlan::RetainConfig => {
                conf.limit = Some(3);
                conf.constraints =
                    Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
            }
            ReturnedPlan::NotDataSourceExec => {
                return Ok(Arc::new(EmptyExec::new(conf.projected_schema()?)));
            }
            ReturnedPlan::NotFileScanConfig => {
                return MemTable::try_new(conf.projected_schema()?, vec![vec![]])?
                    .scan(state, None, &[], None)
                    .await;
            }
            ReturnedPlan::WithoutOrdering => conf.output_ordering.clear(),
            ReturnedPlan::WeakerOrdering => {
                conf.output_ordering =
                    vec![LexOrdering::new(vec![conf.output_ordering[0][0].clone()]).unwrap()];
            }
            ReturnedPlan::FilteredProjection => {
                let predicate = Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("major", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(datafusion::common::ScalarValue::Int32(Some(
                        1,
                    )))),
                ));
                let mut options = state.config_options().clone();
                options.execution.parquet.pushdown_filters = true;
                let source = conf
                    .file_source()
                    .try_pushdown_filters(vec![predicate], &options)?
                    .updated_node
                    .unwrap();
                let projection = ProjectionExprs::from_indices(&[1], conf.file_schema());
                let source = source.try_pushdown_projection(&projection)?.unwrap();
                conf = FileScanConfigBuilder::from(conf)
                    .with_source(source)
                    .build();
            }
            ReturnedPlan::WithoutPartitioning => conf.output_partitioning = None,
            ReturnedPlan::MultipleFileGroups => {
                let files = conf.file_groups[0].files();
                conf.file_groups = files
                    .iter()
                    .cloned()
                    .map(|file| FileGroup::new(vec![file]))
                    .collect();
            }
        }
        self.parquet.create_physical_plan(state, conf).await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.parquet.file_source(table_schema)
    }
}

async fn scan(format: Arc<TestFormat>) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    let store = MemoryStore::new("format-contract");
    store.register(&ctx);
    let table = SortedTable::new(
        store.url().clone(),
        format,
        vec![
            file_with_statistics(
                "later.parquet",
                vec![
                    column_statistics(Some(1), Some(2)),
                    column_statistics(Some(1), Some(5)),
                ],
            ),
            file_with_statistics(
                "earlier.parquet",
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
    table.scan(&ctx.state(), None, &[], Some(9)).await
}

#[tokio::test]
async fn delegates_plan_creation_and_retains_the_formats_returned_config() {
    let format = Arc::new(TestFormat::new(ReturnedPlan::RetainConfig));

    let plan = scan(format.clone()).await.unwrap();

    let calls = format.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let input = &calls[0];
    assert_eq!(input.limit, Some(9));
    assert!(input.constraints.is_empty());
    assert_eq!(input.file_groups.len(), 1);
    let files = input.file_groups[0].files();
    let paths: Vec<_> = files
        .iter()
        .map(|file| file.object_meta.location.as_ref())
        .collect();
    assert_eq!(paths, ["earlier.parquet", "later.parquet"]);
    assert!(
        files
            .iter()
            .all(|file| { file.statistics.as_ref().unwrap().num_rows == Precision::Exact(1) })
    );
    assert_eq!(input.output_ordering.len(), 1);
    assert!(matches!(
        input.output_partitioning,
        Some(Partitioning::UnknownPartitioning(1))
    ));

    assert!(plan.is::<DataSourceExec>());
    // These properties must come from the format's returned config, not its input.
    assert_eq!(plan.fetch(), Some(3));
    assert_eq!(
        plan.equivalence_properties().constraints(),
        &Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    );
    assert_eq!(plan.output_ordering(), input.output_ordering.first());
    assert!(matches!(
        plan.output_partitioning(),
        Partitioning::UnknownPartitioning(1)
    ));
}

async fn assert_contract_error(returned_plan: ReturnedPlan, plan_name: &str) {
    let format = Arc::new(TestFormat::new(returned_plan));

    let error = scan(format.clone()).await.unwrap_err();

    assert_eq!(format.calls.lock().unwrap().len(), 1);
    let DataFusionError::Plan(message) = error else {
        panic!("expected a plan error for {returned_plan:?}, got {error}");
    };
    for required in [
        "format-contract-test",
        "DataSourceExec",
        "FileScanConfig",
        "ordering",
        "UnknownPartitioning(1)",
        "file group",
        plan_name,
    ] {
        assert!(
            message.contains(required),
            "error for {returned_plan:?} must include {required:?}: {message}"
        );
    }
}

#[tokio::test]
async fn rejects_a_format_plan_that_is_not_a_data_source_exec() {
    assert_contract_error(ReturnedPlan::NotDataSourceExec, "EmptyExec").await;
}

#[tokio::test]
async fn rejects_a_data_source_exec_without_a_file_scan_config() {
    assert_contract_error(ReturnedPlan::NotFileScanConfig, "DataSourceExec").await;
}

#[tokio::test]
async fn retains_ordering_when_the_format_returns_a_filtered_projection() {
    let plan = scan(Arc::new(TestFormat::new(ReturnedPlan::FilteredProjection)))
        .await
        .unwrap();
    assert_eq!(
        plan.output_ordering().map(ToString::to_string).as_deref(),
        Some("minor@0 ASC NULLS LAST")
    );
}

#[tokio::test]
async fn rejects_a_format_that_drops_the_declared_ordering() {
    assert_contract_error(ReturnedPlan::WithoutOrdering, "DataSourceExec").await;
}

#[tokio::test]
async fn rejects_a_format_that_weakens_the_declared_ordering() {
    assert_contract_error(ReturnedPlan::WeakerOrdering, "DataSourceExec").await;
}

#[tokio::test]
async fn rejects_a_format_that_drops_the_explicit_single_partition() {
    assert_contract_error(ReturnedPlan::WithoutPartitioning, "DataSourceExec").await;
}

#[tokio::test]
async fn rejects_a_format_that_splits_the_ordered_file_group() {
    assert_contract_error(ReturnedPlan::MultipleFileGroups, "DataSourceExec").await;
}
