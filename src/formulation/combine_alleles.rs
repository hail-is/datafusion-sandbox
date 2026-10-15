use crate::ordered_frame::{OrderedFrame, OutputLayout};
use crate::row_ordering::RowOrdering;
use crate::stored::dataset::Dataset;

use datafusion::{
    error::{DataFusionError, Result},
    functions_window::rank::rank,
    prelude::*,
};

/// The least row ordering of the allele combiner.
pub(super) fn required_ordering() -> RowOrdering {
    RowOrdering::locus_then_alleles()
}

/// How a write of the allele combiner's rows lays them out: one file.
pub(super) const fn output_layout() -> OutputLayout {
    OutputLayout::SingleFile
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
///
/// # Errors
///
/// Returns a plan error if the dataset holds a multi-sample input table, which this combiner does
/// not read, or does not satisfy its ordering.
pub(super) async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<OrderedFrame> {
    if let Some(table) = dataset
        .input_tables()
        .iter()
        .find(|table| table.is_multi_sample())
    {
        return Err(DataFusionError::Plan(format!(
            "the allele combiner reads only single-sample input tables, and multi-sample input \
             table '{}' is in the dataset",
            table.name()
        )));
    }
    let ordering = dataset.query_ordering(&required_ordering())?;
    let columns = ordering.column_names();
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let frame = dataset
        .read(ctx)
        .await?
        .select_columns(&columns)?
        .distinct()?;
    // ADR 0003: preferring existing sort lets this window stack avoid a re-sort.
    let frame = frame.window(vec![
        rank()
            .order_by(ordering.sort_expressions())
            .partition_by(ordering.locus_prefix().partition_expressions())
            .build()?,
    ])?;
    let frame = frame.sort(ordering.sort_expressions())?;
    Ok(OrderedFrame {
        frame,
        ordering,
        layout: output_layout(),
    })
}
