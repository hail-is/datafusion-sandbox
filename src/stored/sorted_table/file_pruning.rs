//! Removes files whose statistics exclude every row an inexact filter could match.
//!
//! The inexact filters are conjoined, coerced and simplified against the table schema, and
//! turned into one physical predicate. The attached scalar is folded into that predicate as a
//! literal, the same way the Parquet and Vortex openers fold partition values, so what reaches
//! `DataFusion`'s file pruner names only file columns. Each file is then tested with the file
//! schema and its own statistics.
//!
//! Pruning and order recovery read the same ordering statistics with different demands. Order
//! recovery trusts a bound for placement, so a compared bound must be exact; see `file_order` and
//! ADR 0011. Pruning only ever removes a file whose bounds prove the predicate false, so any bound
//! that is not proof keeps the file. `DataFusion`'s pruner reads exact bounds only and treats
//! inexact or absent ones as unknown, so a truncated string bound never excludes a file, a file
//! with no statistics is kept, and a predicate the pruner cannot rewrite keeps every file.
//!
//! Formats fold a column that is constant within a file into a literal before their own pruner
//! runs, so a filter on such a column never excludes the file at execution time. Here the
//! constant shows up as equal exact bounds, and the pruner excludes the file from the plan.

use super::{AttachedScalar, expr_simplifier};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::{DFSchema, Result},
    datasource::{listing::PartitionedFile, table_schema::TableSchema},
    logical_expr::{Expr, utils::conjunction},
    physical_expr::{PhysicalExpr, simplifier::PhysicalExprSimplifier},
    physical_expr_adapter::replace_columns_with_literals,
    physical_optimizer::pruning::FilePruner,
    physical_plan::metrics::Count,
};
use std::{collections::HashMap, sync::Arc};

/// The predicate every retained file might satisfy.
#[derive(Debug)]
pub(super) struct FilePruning {
    predicate: Arc<dyn PhysicalExpr>,
    file_schema: SchemaRef,
}

impl FilePruning {
    /// Builds the predicate from `inexact_filters`, or returns `None` when there are none.
    pub(super) fn new(
        state: &dyn Session,
        table_schema: &TableSchema,
        attached: Option<&AttachedScalar>,
        inexact_filters: Vec<Expr>,
    ) -> Result<Option<Self>> {
        let Some(filter) = conjunction(inexact_filters) else {
            return Ok(None);
        };
        let df_schema = Arc::new(DFSchema::try_from(Arc::clone(table_schema.table_schema()))?);
        // Coercion casts literals to the column types where it can, and simplification
        // unwraps casts from columns, so the pruner sees bounds and literals of one type.
        let simplifier = expr_simplifier(state, &df_schema);
        let coerced = simplifier.coerce(filter, &df_schema)?;
        let filter = simplifier.simplify(coerced)?;
        let predicate = state.create_physical_expr(filter, &df_schema)?;
        // The attached scalar is the last table column, so folding it leaves every remaining
        // column reference pointing at its file schema index.
        let literals: HashMap<&str, _> = attached
            .iter()
            .map(|attached| (attached.field.name().as_str(), attached.value.clone()))
            .collect();
        let predicate = replace_columns_with_literals(predicate, &literals)?;
        let file_schema = Arc::clone(table_schema.file_schema());
        let predicate = PhysicalExprSimplifier::new(&file_schema).simplify(predicate)?;
        Ok(Some(Self {
            predicate,
            file_schema,
        }))
    }

    /// Keeps `files` in their given order, minus those whose statistics prove the predicate
    /// false for every row.
    pub(super) fn retain(&self, files: Vec<PartitionedFile>) -> Result<Vec<PartitionedFile>> {
        let mut retained = Vec::with_capacity(files.len());
        for file in files {
            if !self.excludes(&file)? {
                retained.push(file);
            }
        }
        Ok(retained)
    }

    fn excludes(&self, file: &PartitionedFile) -> Result<bool> {
        // The pruner counts predicates it could not build instead of failing; the count is
        // not observed because an unusable predicate keeps the file, which is the intent.
        let Some(mut pruner) = FilePruner::try_new(
            Arc::clone(&self.predicate),
            &self.file_schema,
            file,
            Count::new(),
        ) else {
            return Ok(false);
        };
        pruner.should_prune()
    }
}
