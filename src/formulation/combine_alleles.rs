use crate::dataset::Dataset;
use crate::locus::LocusOrdering;

use datafusion::{error::Result, functions_window::rank::rank, prelude::*};

pub fn required_ordering() -> LocusOrdering {
    LocusOrdering::locus_then_alleles()
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let query_ordering = dataset.query_ordering(&required_ordering())?;
    let df = dataset
        .read(ctx)
        .await?
        .select_columns(&query_ordering.column_names())?
        .distinct()?;
    // ADR 0003: preferring existing sort lets this window stack avoid a re-sort.
    let df = df.window(vec![
        rank()
            .order_by(query_ordering.sort_expressions())
            .partition_by(query_ordering.locus_prefix().partition_expressions())
            .build()?,
    ])?;
    df.sort(query_ordering.sort_expressions())
}
