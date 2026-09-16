use crate::dataset::Dataset;
use crate::locus::{LocusOrdering, StoredOrdering};

use datafusion::{error::Result, functions_window::rank::rank, prelude::*};

pub fn required_ordering() -> LocusOrdering {
    LocusOrdering::locus_then_alleles()
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<(DataFrame, StoredOrdering)> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let frame = dataset
        .read(ctx)
        .await?
        .select_columns(&ordering.column_names())?
        .distinct()?;
    // ADR 0003: preferring existing sort lets this window stack avoid a re-sort.
    let frame = frame.window(vec![
        rank()
            .order_by(ordering.sort_expressions())
            .partition_by(ordering.locus_prefix().partition_expressions())
            .build()?,
    ])?;
    let frame = frame.sort(ordering.sort_expressions())?;
    Ok((frame, ordering))
}
