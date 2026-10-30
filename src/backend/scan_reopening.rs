//! Scan reopening: the async pass that turns a decoded plan's described scans back into the scans
//! they describe.
//!
//! Decoding plan bytes leaves each scan a [`DescribedScan`], a placeholder holding its
//! description, since a decoder can't do IO. [`reopen`] then discovers each dataset the plan reads
//! and swaps every placeholder for the sorted-table scan of its input table that the Rust
//! formulations read. Discovery lists the dataset's files afresh, so a scan reads the files its
//! input table holds when it is reopened, not when it was described.

use crate::{
    backend::scan_description::{DescribedScan, ScanDescription},
    format::InputFormat,
    row_ordering::RowOrdering,
    stored::dataset::Dataset,
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        DataFusionError,
        tree_node::{Transformed, TreeNodeRecursion},
    },
    datasource::{listing::ListingTableUrl, source_as_provider},
    error::Result,
    logical_expr::{LogicalPlan, TableScan, TableSource},
    prelude::SessionContext,
};
use std::{collections::HashMap, slice, sync::Arc};

/// The scan descriptions `plan` reads, each once, in the order the plan first reads them,
/// subqueries included.
///
/// # Errors
///
/// Returns an error if the plan reads a table that is not a scan description.
pub fn descriptions(plan: &LogicalPlan) -> Result<Vec<ScanDescription>> {
    let mut descriptions = Vec::<ScanDescription>::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let (description, _) = described(scan)?;
            if !descriptions.contains(&description) {
                descriptions.push(description);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(descriptions)
}

/// `plan` with every described scan reopened through `ctx` as the sorted-table scan of its input
/// table, listing the input table's files now. Each dataset is discovered once.
///
/// # Errors
///
/// Returns an error if the plan reads a table that is not a scan description, a dataset cannot be
/// discovered or no longer has a described input table, or an input table's scan reads a
/// different schema than its description was handed out with.
pub async fn reopen(ctx: &SessionContext, plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut datasets = HashMap::<(String, InputFormat, RowOrdering), Dataset>::new();
    let mut sources = HashMap::<ScanDescription, Arc<dyn TableSource>>::new();
    for description in descriptions(&plan)? {
        let key = (
            description.dataset_path.clone(),
            description.input_format.clone(),
            description.row_ordering.clone(),
        );
        if !datasets.contains_key(&key) {
            let dataset = Dataset::discover(
                ctx,
                ListingTableUrl::parse(&description.dataset_path)?,
                description.input_format.clone(),
                description.row_ordering.clone(),
                None,
            )
            .await?;
            datasets.insert(key.clone(), dataset);
        }
        let dataset = datasets.get(&key).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "dataset '{}' was discovered but not kept",
                description.dataset_path
            ))
        })?;
        let read = dataset
            .restrict_to(slice::from_ref(&description.input_table))?
            .read(ctx)
            .await?
            .into_unoptimized_plan();
        let LogicalPlan::TableScan(scan) = read else {
            return Err(DataFusionError::Internal(format!(
                "input table '{}' of dataset '{}' read as a plan that is not one scan: {read}",
                description.input_table, description.dataset_path
            )));
        };
        sources.insert(description, scan.source);
    }
    plan.transform_up_with_subqueries(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(Transformed::no(node));
        };
        let (description, schema) = described(&scan)?;
        let source = sources.get(&description).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "input table '{}' of dataset '{}' was not reopened",
                description.input_table, description.dataset_path
            ))
        })?;
        if source.schema() != schema {
            return Err(DataFusionError::Plan(format!(
                "input table '{}' of dataset '{}' reads schema {:?} now, and its scan description \
                 was handed out with {:?}",
                description.input_table,
                description.dataset_path,
                source.schema(),
                schema
            )));
        }
        Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
            source: Arc::clone(source),
            ..scan
        })))
    })
    .map(|transformed| transformed.data)
}

/// The description of the described scan `scan` reads, and the schema it was handed out with.
fn described(scan: &TableScan) -> Result<(ScanDescription, SchemaRef)> {
    let provider = source_as_provider(&scan.source)?;
    let Some(described) = provider.downcast_ref::<DescribedScan>() else {
        return Err(DataFusionError::Plan(format!(
            "the plan reads table '{}', which is not a scan description",
            scan.table_name
        )));
    };
    Ok((
        described.description().clone(),
        Arc::clone(described.schema_ref()),
    ))
}
