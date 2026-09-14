//! A file-backed table that scans its files as one statistics-ordered partition.
//!
//! This provider composes public listing-table building blocks, not a `ListingTable`.
//! Its data source wrapper preserves recovered ordering through physical optimization;
//! plan creation and execution stay with the format. See ADR 0012.
//!
//! Eligible attached-scalar filters are exact and evaluated before collecting file metadata.
//! False or SQL-null filters produce an empty plan over the projected schema. Other filters
//! are inexact: once every file has statistics, they prune the files those statistics exclude,
//! and they keep a logical residual for `DataFusion`'s physical filter pushdown to the format.
//! Pruning runs before zero-row dropping and order recovery, so a filter can remove a file whose
//! ordering bounds order recovery could not use. See `file_pruning` for why pruning and order
//! recovery make different demands of the same bounds.
//!
//! Pruning follows metadata collection rather than preceding it. The one per-file constant the
//! table knows before any read is the attached scalar, and exact filters already answer it. A
//! stored column that is constant within a file reveals its value only through that file's
//! statistics, and the table infers nothing from file names or paths. So on a cold cache a
//! contig filter still reads every footer once; the saving is the excluded files' data reads.
//! Attached and cached statistics need no footer read, and an excluded file is absent from the
//! file group, so execution never opens it.
//!
//! Per-file statistics come from the caller, the runtime's file statistics cache, or footer
//! inference, in that order of precedence; see `statistics_source`. The scan's table statistics
//! aggregate the files in its file group with `DataFusion`'s helper, so a plan knows what the
//! footers told it rather than claiming unknown statistics.
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

mod file_pruning;
mod ordered_source;
mod statistics_source;

use file_pruning::FilePruning;
use ordered_source::OrderedSource;
use statistics_source::StatisticsSource;

use crate::file_order::{Bounds, Direction, FileOrderError, recover_file_order};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::{FieldRef, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    common::{
        DFSchema, DataFusionError, Result, ScalarValue,
        stats::Precision,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::{
        file_format::FileFormat,
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::{
        Expr, SortExpr, TableProviderFilterPushDown, TableType, Volatility,
        simplify::SimplifyContext,
    },
    optimizer::simplify_expressions::ExprSimplifier,
    physical_expr::create_lex_ordering,
    physical_plan::{ExecutionPlan, Partitioning, empty::EmptyExec},
};
use datafusion_datasource::compute_all_files_statistics;
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
    statistics_source: StatisticsSource,
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
        let statistics_source = StatisticsSource::new(
            Arc::clone(&format),
            object_store_url.clone(),
            Arc::clone(&file_schema),
        );
        Self {
            object_store_url,
            format,
            files,
            file_schema,
            table_schema,
            ordering,
            attached_scalar,
            statistics_source,
        }
    }

    /// Every file with its statistics, extended by the attached scalar's partition value.
    async fn files_with_statistics(&self, state: &dyn Session) -> Result<Vec<PartitionedFile>> {
        let files = self
            .files
            .iter()
            .cloned()
            .map(|mut file| {
                file.partition_values = self
                    .attached_scalar
                    .iter()
                    .map(|attached| attached.value.clone())
                    .collect();
                file
            })
            .collect();
        self.statistics_source.for_files(state, files).await
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

fn is_exact_filter(attached: Option<&AttachedScalar>, filter: &Expr) -> Result<bool> {
    let Some(attached) = attached else {
        return Ok(false);
    };
    let mut exact = true;
    filter.apply(|expr| {
        exact = match expr {
            Expr::Column(column) => column.name == *attached.field.name(),
            // Unlike listing-table pruning, evaluation here has the session context
            // needed by stable functions. Only volatile functions are excluded.
            Expr::ScalarFunction(function) => {
                function.func.signature().volatility != Volatility::Volatile
            }
            Expr::HigherOrderFunction(function) => {
                function.func.signature().volatility != Volatility::Volatile
            }
            #[expect(
                deprecated,
                reason = "wildcards must not be evaluated as scalar filters"
            )]
            Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Wildcard { .. }
            | Expr::Unnest(_)
            | Expr::Placeholder(_)
            // These depend on a query or external values, not just the attached
            // scalar. Expression traversal does not visit subquery plans.
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::SetComparison(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _) => false,
            _ => true,
        };
        Ok(if exact {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(exact)
}

/// Splits `filters` into the exact ones, which reference only the attached scalar, and the
/// inexact rest.
fn classify_filters(
    attached: Option<&AttachedScalar>,
    filters: &[Expr],
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut exact = Vec::new();
    let mut inexact = Vec::new();
    for filter in filters {
        if is_exact_filter(attached, filter)? {
            exact.push(filter.clone());
        } else {
            inexact.push(filter.clone());
        }
    }
    Ok((exact, inexact))
}

/// A simplifier over `df_schema` that knows the session's options and the query's start time,
/// so functions such as `current_date` fold before a physical expression evaluates them.
fn expr_simplifier(state: &dyn Session, df_schema: &Arc<DFSchema>) -> ExprSimplifier {
    ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(Arc::clone(df_schema))
            .with_config_options(Arc::new(state.config_options().clone()))
            .with_query_execution_start_time(state.execution_props().query_execution_start_time)
            .build(),
    )
}

/// Evaluates the exact filters against the attached scalar. Inexact filters must not reach
/// here: they cannot be evaluated against the scalar-only batch.
fn scalar_filters_match(
    attached: Option<&AttachedScalar>,
    state: &dyn Session,
    exact_filters: &[Expr],
) -> Result<bool> {
    let Some(attached) = attached else {
        return Ok(true);
    };
    if exact_filters.is_empty() {
        return Ok(true);
    }
    let schema = Arc::new(Schema::new(vec![Arc::clone(&attached.field)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![attached.value.to_array_of_size(1)?],
    )?;
    let df_schema = Arc::new(DFSchema::try_from(schema)?);
    let simplifier = expr_simplifier(state, &df_schema);
    for filter in exact_filters {
        let coerced = simplifier.coerce(filter.clone(), &df_schema)?;
        let predicate = simplifier.simplify(coerced)?;
        let expression = state.create_physical_expr(predicate, &df_schema)?;
        let value = expression.evaluate(&batch)?.into_array(1)?;
        match ScalarValue::try_from_array(&value, 0)? {
            ScalarValue::Boolean(Some(true)) => {}
            ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => return Ok(false),
            value => {
                return Err(DataFusionError::Plan(format!(
                    "sorted table filter '{filter}' must return Boolean, got {}",
                    value.data_type(),
                )));
            }
        }
    }
    Ok(true)
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                Ok(if is_exact_filter(self.attached_scalar.as_ref(), filter)? {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                })
            })
            .collect()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let attached = self.attached_scalar.as_ref();
        let (exact_filters, inexact_filters) = classify_filters(attached, filters)?;
        if !scalar_filters_match(attached, state, &exact_filters)? {
            let schema = projection.map_or_else(
                || Ok(self.schema()),
                |indices| self.schema().project(indices).map(Arc::new),
            )?;
            return Ok(Arc::new(EmptyExec::new(schema)));
        }
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
        let files = match FilePruning::new(state, &self.table_schema, attached, inexact_filters)? {
            Some(pruning) => pruning.retain(files)?,
            None => files,
        };
        let file_group = self.ordered_file_group(files)?;
        // The scan holds exactly the files in this group, so their aggregate is its table
        // statistics. Every file already has statistics, because order recovery needs them,
        // so aggregating costs nothing even when the session does not collect statistics.
        // The group is never truncated by the limit, so the aggregate keeps each input's
        // precision rather than being marked inexact.
        let collect_statistics = true;
        let truncated_by_limit = false;
        let (mut file_groups, statistics) = compute_all_files_statistics(
            vec![file_group],
            Arc::clone(self.table_schema.table_schema()),
            collect_statistics,
            truncated_by_limit,
        )?;
        let file_group = file_groups.pop().ok_or_else(|| {
            DataFusionError::Internal(
                "statistics aggregation returned no file group for the sorted table".to_string(),
            )
        })?;
        let source = self.format.file_source(self.table_schema.clone());
        let scan_config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(file_group)
            .with_statistics(statistics)
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
