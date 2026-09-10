//! A file-backed table that scans its files as one statistics-ordered partition.
//!
//! This provider composes public listing-table building blocks, not a `ListingTable`.
//! Its data source wrapper preserves recovered ordering through physical optimization;
//! plan creation and execution stay with the format. See ADR 0012.
//!
//! Deliberately excluded listing-table capabilities:
//! - File-group repartitioning contradicts the single ordered partition, per ADR 0007.
//! - Path-derived Hive columns are unnecessary; the caller supplies the attached scalar.
//! - Listing and schema inference belong to the dataset, per ADR 0005.
//! - Table writes go through the format's sink instead.
//! - Footer sort order is not ordering evidence: our writers do not record it, Vortex
//!   has no equivalent, and ADR 0011 already trusts the writer beyond that evidence.
//! - Schema drift adapters would hide mismatched files; a dataset declares one schema
//!   and mismatches should fail rather than be adapted.

mod ordered_source;

use ordered_source::OrderedSource;

use crate::file_order::{Bounds, Direction, FileOrderError, recover_file_order};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{FieldRef, SchemaRef},
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result, ScalarValue, Statistics, stats::Precision},
    datasource::{
        file_format::FileFormat,
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Expr, SortExpr, TableType},
    physical_expr::create_lex_ordering,
    physical_plan::{ExecutionPlan, Partitioning},
};
use std::sync::Arc;

/// A scalar column appended to every row in a sorted table.
#[derive(Clone, Debug)]
pub struct AttachedScalar {
    pub field: FieldRef,
    pub value: ScalarValue,
}

/// A table whose files are assumed to hold one sorted table, scanned in the order their
/// statistics recover. Files whose statistics refute every sorted order are a plan error.
#[derive(Debug)]
pub struct SortedTable {
    object_store_url: ObjectStoreUrl,
    format: Arc<dyn FileFormat>,
    files: Vec<PartitionedFile>,
    file_schema: SchemaRef,
    table_schema: TableSchema,
    ordering: Vec<SortExpr>,
    attached_scalar: Option<AttachedScalar>,
}

impl SortedTable {
    pub fn new(
        object_store_url: ObjectStoreUrl,
        format: Arc<dyn FileFormat>,
        files: Vec<PartitionedFile>,
        file_schema: SchemaRef,
        ordering: Vec<SortExpr>,
        attached_scalar: Option<AttachedScalar>,
    ) -> Self {
        let partition_fields: Vec<_> = attached_scalar
            .iter()
            .map(|attached| Arc::clone(&attached.field))
            .collect();
        let table_schema = TableSchema::builder(Arc::clone(&file_schema))
            .with_table_partition_cols(partition_fields)
            .build();
        Self {
            object_store_url,
            format,
            files,
            file_schema,
            table_schema,
            ordering,
            attached_scalar,
        }
    }

    async fn files_with_statistics(&self, state: &dyn Session) -> Result<Vec<PartitionedFile>> {
        let mut files = Vec::with_capacity(self.files.len());
        for mut file in self.files.clone() {
            let statistics = if let Some(statistics) = file.statistics.take() {
                statistics
            } else {
                let store = state.runtime_env().object_store(&self.object_store_url)?;
                Arc::new(
                    self.format
                        .infer_stats(
                            state,
                            &store,
                            Arc::clone(&self.file_schema),
                            &file.object_meta,
                        )
                        .await?,
                )
            };
            file.partition_values = self
                .attached_scalar
                .iter()
                .map(|attached| attached.value.clone())
                .collect();
            files.push(file.with_statistics(statistics));
        }
        Ok(files)
    }

    /// The file-schema index and direction of every ordering column.
    ///
    /// The attached scalar is a table column but not a file column, so ordering by it is an
    /// error here rather than a constant bound.
    fn ordering_columns(&self) -> Result<Vec<OrderingColumn>> {
        self.ordering
            .iter()
            .map(|sort| {
                let Expr::Column(column) = &sort.expr else {
                    return Err(DataFusionError::Plan(format!(
                        "sorted table ordering expression '{}' is not a column",
                        sort.expr
                    )));
                };
                let index = self
                    .file_schema
                    .fields()
                    .iter()
                    .position(|field| field.name() == &column.name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "sorted table ordering column '{}' is not in the file schema",
                            column.name
                        ))
                    })?;
                Ok(OrderingColumn {
                    name: column.name.clone(),
                    index,
                    direction: if sort.asc {
                        Direction::Ascending
                    } else {
                        Direction::Descending
                    },
                })
            })
            .collect()
    }

    /// Drops files with exactly zero rows and places the rest in the order their statistics
    /// recover. See `file_order` and ADR 0011 for what that order trusts.
    fn ordered_file_group(&self, files: Vec<PartitionedFile>) -> Result<FileGroup> {
        let columns = self.ordering_columns()?;
        let files: Vec<PartitionedFile> = files
            .into_iter()
            .filter(|file| !has_zero_rows(file))
            .collect();
        let bounds: Vec<Vec<Bounds>> = files
            .iter()
            .map(|file| columns.iter().map(|column| column.bounds(file)).collect())
            .collect();
        let directions: Vec<Direction> = columns.iter().map(|column| column.direction).collect();
        let order = recover_file_order(&bounds, &directions)
            .map_err(|error| plan_error(error, &files, &columns))?;

        let mut unplaced: Vec<Option<PartitionedFile>> = files.into_iter().map(Some).collect();
        let ordered = order
            .iter()
            .map(|&index| {
                unplaced
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "recovered file order names file {index} twice or out of range"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(FileGroup::new(ordered))
    }
}

struct OrderingColumn {
    name: String,
    index: usize,
    direction: Direction,
}

impl OrderingColumn {
    fn bounds(&self, file: &PartitionedFile) -> Bounds {
        file.statistics
            .as_ref()
            .and_then(|statistics| statistics.column_statistics.get(self.index))
            .map_or(
                Bounds {
                    min: Precision::Absent,
                    max: Precision::Absent,
                },
                |column| Bounds {
                    min: column.min_value.clone(),
                    max: column.max_value.clone(),
                },
            )
    }
}

fn has_zero_rows(file: &PartitionedFile) -> bool {
    file.statistics
        .as_ref()
        .is_some_and(|statistics| statistics.num_rows == Precision::Exact(0))
}

fn plan_error(
    error: FileOrderError,
    files: &[PartitionedFile],
    columns: &[OrderingColumn],
) -> DataFusionError {
    let path = |index: usize| {
        files.get(index).map_or_else(
            || format!("#{index}"),
            |file| file.object_meta.location.to_string(),
        )
    };
    match error {
        FileOrderError::UnusableStatistics { file, column } => {
            let column = columns
                .get(column)
                .map_or_else(|| format!("#{column}"), |column| column.name.clone());
            DataFusionError::Plan(format!(
                "sorted table file '{}' has no exact bounds on ordering column '{column}'",
                path(file)
            ))
        }
        FileOrderError::Refuted { first, second } => DataFusionError::Plan(format!(
            "sorted table files '{}' and '{}' have statistics that refute every sorted order",
            path(first),
            path(second)
        )),
    }
}

#[async_trait]
impl TableProvider for SortedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.table_schema.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The default `supports_filters_pushdown` marks every filter unsupported, so this
        // slice is empty and DataFusion evaluates filters above the scan. We could later
        // advertise support here for statistics-based file pruning or format-level pushdown.
        let output_ordering = create_lex_ordering(
            self.table_schema.table_schema(),
            std::slice::from_ref(&self.ordering),
            state.execution_props(),
        )?;
        if output_ordering.is_empty() {
            return Err(DataFusionError::Plan(
                "sorted table requires an ordering".to_string(),
            ));
        }
        let files = self.files_with_statistics(state).await?;
        let file_group = self.ordered_file_group(files)?;
        let source = self.format.file_source(self.table_schema.clone());
        let scan_config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(file_group)
            .with_statistics(Statistics::new_unknown(self.table_schema.table_schema()))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_output_ordering(output_ordering.clone())
            .with_output_partitioning(Some(Partitioning::UnknownPartitioning(1)))
            .build();
        let plan = self.format.create_physical_plan(state, scan_config).await?;
        let incompatible = || {
            DataFusionError::Plan(format!(
                "sorted table format '{}' must return a DataSourceExec containing a FileScanConfig with the declared ordering and one UnknownPartitioning(1) file group; cannot preserve recovered ordering in plan '{}'",
                self.format.get_ext(),
                plan.name(),
            ))
        };
        let exec = plan
            .downcast_ref::<DataSourceExec>()
            .ok_or_else(incompatible)?;
        let config = exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .ok_or_else(incompatible)?;
        if config.file_groups.len() != 1
            || !matches!(
                config.output_partitioning,
                Some(Partitioning::UnknownPartitioning(1))
            )
            || output_ordering
                .iter()
                .any(|ordering| !config.output_ordering.contains(ordering))
        {
            return Err(incompatible());
        }
        let source = OrderedSource::new(config.clone())?;
        Ok(Arc::new(exec.clone().with_data_source(Arc::new(source))))
    }
}
