//! Exact row-balanced split points from a stored locus-sorted table.
//!
//! The path-taking entry runs two plans over the table: the first obtains the row count, the second
//! reads only the stored ordering's columns, numbers rows in the table's recovered order, and
//! filters to the requested interval boundaries. Results are keyed by row number, so the entry
//! does not depend on the order in which the filtered rows arrive.

use crate::{
    format::InputFormat,
    locus::{Locus, LocusOrdering, SplitPoints},
    stored::locus_sorted_table::LocusSortedTable,
};

use datafusion::{
    common::{DataFusionError, Result, cast::as_uint64_array},
    datasource::listing::ListingTableUrl,
    functions_window::row_number::row_number,
    logical_expr::ExprFunctionExt,
    prelude::{DataFrame, SessionContext, col, lit},
};
use std::{collections::BTreeMap, num::NonZeroUsize};

const ROW_NUMBER_COLUMN: &str = "__row_number";

/// Computes the row-balanced split points of the locus-sorted table at `table_path`.
///
/// # Errors
///
/// Returns an error if `intervals` is less than 2, the path cannot be opened as one locus-sorted
/// table, the table has no rows, the requested intervals select fewer than one distinct row apiece,
/// a selected row does not hold a valid locus, or the selected loci are not strictly increasing.
pub async fn row_balanced(
    ctx: &SessionContext,
    table_path: ListingTableUrl,
    input_format: InputFormat,
    intervals: NonZeroUsize,
) -> Result<SplitPoints> {
    validate_interval_count(intervals)?;
    let table =
        LocusSortedTable::open(ctx, table_path, input_format, LocusOrdering::locus()).await?;
    let (targets, selected) = selected_rows(ctx, &table, intervals).await?;
    let batches = selected.collect().await?;
    let representation = table.locus_representation();
    let mut loci_by_row_number = BTreeMap::new();
    for batch in &batches {
        let loci = representation.loci(batch).map_err(configuration_error)?;
        let row_number_column = batch.column_by_name(ROW_NUMBER_COLUMN).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "row-balanced plan did not return its '{ROW_NUMBER_COLUMN}' column"
            ))
        })?;
        let row_numbers = as_uint64_array(row_number_column.as_ref())?;
        for (row_number, locus) in row_numbers.iter().zip(loci) {
            let row_number = row_number.ok_or_else(|| {
                DataFusionError::Internal(
                    "row-balanced plan returned a null row number".to_string(),
                )
            })?;
            loci_by_row_number.insert(row_number, locus);
        }
    }
    let loci = targets
        .iter()
        .map(|target| {
            loci_by_row_number.get(target).copied().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "row-balanced plan did not return target row number {target}"
                ))
            })
        })
        .collect::<Result<Vec<Locus>>>()?;
    SplitPoints::new(loci).map_err(|error| match error {
        DataFusionError::Configuration(message) => DataFusionError::Configuration(format!(
            "the table cannot be balanced into {} intervals: {message}",
            intervals.get()
        )),
        other => other,
    })
}

/// Counts the table's rows and selects the target rows by their row number in locus order.
async fn selected_rows(
    ctx: &SessionContext,
    table: &LocusSortedTable,
    intervals: NonZeroUsize,
) -> Result<(Vec<u64>, DataFrame)> {
    let frame = table.read(ctx)?;
    let row_count = frame.clone().count().await?;
    if row_count == 0 {
        return Err(DataFusionError::Configuration(
            "the table has no rows".to_string(),
        ));
    }

    let targets = target_row_numbers(row_count, intervals)?;
    let ordering = table.stored_ordering();
    let columns = ordering.column_names();
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let selected = frame
        .select_columns(&columns)?
        .window(vec![
            row_number()
                .order_by(ordering.sort_expressions())
                .build()?
                .alias(ROW_NUMBER_COLUMN),
        ])?
        .filter(
            col(ROW_NUMBER_COLUMN)
                .in_list(targets.iter().copied().map(lit).collect::<Vec<_>>(), false),
        )?;
    Ok((targets, selected))
}

fn target_row_numbers(row_count: usize, intervals: NonZeroUsize) -> Result<Vec<u64>> {
    let row_count = u128::try_from(row_count).map_err(|error| {
        DataFusionError::Internal(format!(
            "table row count {row_count} cannot be represented for split-point arithmetic: {error}"
        ))
    })?;
    let intervals = u128::try_from(intervals.get()).map_err(|error| {
        DataFusionError::Internal(format!(
            "interval count {} cannot be represented for split-point arithmetic: {error}",
            intervals.get()
        ))
    })?;
    (1..intervals)
        .map(|k| {
            let index = k
                .checked_mul(row_count)
                .and_then(|product| product.checked_div(intervals))
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "overflow while computing a split-point row index".to_string(),
                    )
                })?;
            let row_number = index.checked_add(1).ok_or_else(|| {
                DataFusionError::Internal(
                    "overflow while converting a split-point index to a row number".to_string(),
                )
            })?;
            let row_number = u64::try_from(row_number).map_err(|error| {
                DataFusionError::Internal(format!(
                    "split-point row number cannot be represented as UInt64: {error}"
                ))
            })?;
            Ok(row_number)
        })
        .collect()
}

fn validate_interval_count(intervals: NonZeroUsize) -> Result<()> {
    if intervals.get() < 2 {
        return Err(DataFusionError::Configuration(
            "interval count must be at least 2".to_string(),
        ));
    }
    Ok(())
}

fn configuration_error(error: DataFusionError) -> DataFusionError {
    match error {
        DataFusionError::Plan(message) | DataFusionError::Configuration(message) => {
            DataFusionError::Configuration(message)
        }
        other => other,
    }
}
