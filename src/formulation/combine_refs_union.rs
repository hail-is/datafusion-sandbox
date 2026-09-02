use crate::dataset::Dataset;
use crate::locus::LocusOrdering;

use datafusion::{error::Result, prelude::*};

pub fn required_ordering() -> LocusOrdering {
    LocusOrdering::locus()
}

/// Builds the union-of-per-sample-scans formulation.
///
/// Many `DataSourceExec`s feed a `UnionExec`, which feeds a
/// `SortPreservingMergeExec` with one partition per input sample.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let query_ordering = dataset.query_ordering(&required_ordering())?;
    dataset
        .read(ctx)
        .await?
        .sort(query_ordering.sort_expressions())
}
