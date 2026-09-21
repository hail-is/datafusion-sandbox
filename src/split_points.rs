//! Exact row-balanced split points from a stored locus-sorted table.
//!
//! The path-taking entry runs both plans through the pipeline runner. The first obtains the row
//! count. The second reads only locus fields, numbers rows in the table's recovered order, and
//! filters to the requested interval boundaries. Keeping the plan as scan, window, and filter
//! avoids collecting the table or reading its locus columns more than once.

use crate::{
    dataset::read_sorted_table,
    format::InputFormat,
    locus::{Locus, LocusOrdering, LocusRepresentation, SplitPoints},
    pipeline::{self, PipelineOptions},
};

use datafusion::{
    common::{DataFusionError, Result, cast::as_uint64_array},
    datasource::listing::ListingTableUrl,
    functions_window::row_number::row_number,
    logical_expr::ExprFunctionExt,
    prelude::{DataFrame, col, lit},
};
use std::{collections::BTreeMap, num::NonZeroUsize};

const ROW_NUMBER_COLUMN: &str = "__row_number";

/// Computes the row-balanced split points of the sorted table at `path`.
///
/// # Errors
///
/// Returns an error if the path cannot be read as one locus-sorted table, the table has no rows,
/// the requested intervals select fewer than one distinct row apiece, a selected row does not hold
/// a valid locus, or the selected loci are not strictly increasing.
pub fn row_balanced(
    path: String,
    input_format: InputFormat,
    intervals: NonZeroUsize,
    threads: NonZeroUsize,
) -> Result<SplitPoints> {
    validate_interval_count(intervals)?;
    let options = PipelineOptions::for_paths(threads, [path.as_str()])?;
    pipeline::run(
        move |ctx| async move {
            let table_path = ListingTableUrl::parse(path)?;
            let frame =
                read_sorted_table(&ctx, table_path, input_format, LocusOrdering::locus()).await?;
            row_balanced_from_frame(frame, intervals).await
        },
        options,
    )
}

/// Computes row-balanced split points from an already resolved sorted frame.
///
/// This is the exact method's internal seam for module tests and callers that already have a
/// frame. Plan execution still belongs inside [`pipeline::run`].
pub(crate) async fn row_balanced_from_frame(
    frame: DataFrame,
    intervals: NonZeroUsize,
) -> Result<SplitPoints> {
    validate_interval_count(intervals)?;
    let (representation, targets, selected) = row_balanced_plan(frame, intervals).await?;
    let batches = selected.collect().await?;
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

/// Builds the exact method's window and filter plan after executing its row-count plan.
pub(crate) async fn row_balanced_plan(
    frame: DataFrame,
    intervals: NonZeroUsize,
) -> Result<(LocusRepresentation, Vec<u64>, DataFrame)> {
    let representation = LocusRepresentation::detect(frame.schema().inner())?;
    let ordering = LocusOrdering::locus().expand(representation);
    let row_count = frame.clone().count().await?;
    if row_count == 0 {
        return Err(DataFusionError::Configuration(
            "the table has no rows".to_string(),
        ));
    }

    let targets = target_row_numbers(row_count, intervals)?;
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

    // DataFusion otherwise repartitions above the global window to parallelize the filter. That
    // would violate the one-partition plan and allow collect to return target rows out of order.
    let (mut state, plan) = selected.into_parts();
    state
        .config_mut()
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    Ok((representation, targets, DataFrame::new(state, plan)))
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
