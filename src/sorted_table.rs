//! A file-backed table that scans its files as one statistics-ordered partition.

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{FieldRef, SchemaRef},
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result, ScalarValue, Statistics},
    datasource::{
        file_format::FileFormat,
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder},
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

/// A table whose files form one non-overlapping ordered sequence.
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
            let statistics = match file.statistics.take() {
                Some(statistics) => statistics,
                None => {
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
                }
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

    fn ordered_file_group(
        &self,
        files: Vec<PartitionedFile>,
        ordering: &datafusion::physical_expr::LexOrdering,
    ) -> Result<FileGroup> {
        // Validate files individually so DataFusion's generic statistics error can name its file.
        for file in &files {
            let single_file_group = [FileGroup::new(vec![file.clone()])];
            if let Err(error) = FileScanConfig::split_groups_by_statistics_with_target_partitions(
                self.table_schema.table_schema(),
                &single_file_group,
                ordering,
                1,
            ) {
                return Err(DataFusionError::Plan(format!(
                    "sorted table file '{}' has no usable ordering statistics: {error}",
                    file.object_meta.location
                )));
            }
        }

        let groups = FileScanConfig::split_groups_by_statistics_with_target_partitions(
            self.table_schema.table_schema(),
            &[FileGroup::new(files)],
            ordering,
            1,
        )?;
        match groups.as_slice() {
            [] => Ok(FileGroup::new(vec![])),
            [group] => Ok(group.clone()),
            [_, overlapping_groups @ ..] => {
                let offending_file = overlapping_groups
                    .iter()
                    .flat_map(FileGroup::iter)
                    .next()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "statistics splitting produced an empty overlap group".to_string(),
                        )
                    })?;
                Err(DataFusionError::Plan(format!(
                    "sorted table file '{}' overlaps another file in its declared ordering",
                    offending_file.object_meta.location
                )))
            }
        }
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
        let ordering = output_ordering.first().ok_or_else(|| {
            DataFusionError::Plan("sorted table requires an ordering".to_string())
        })?;
        let files = self.files_with_statistics(state).await?;
        let file_group = self.ordered_file_group(files, ordering)?;
        let source = self.format.file_source(self.table_schema.clone());
        let scan_config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(file_group)
            .with_statistics(Statistics::new_unknown(self.table_schema.table_schema()))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_output_ordering(output_ordering)
            .with_output_partitioning(Some(Partitioning::UnknownPartitioning(1)))
            .build();
        self.format.create_physical_plan(state, scan_config).await
    }
}
