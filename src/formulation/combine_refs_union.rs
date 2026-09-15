use crate::dataset::{Dataset, ScanShape};
use crate::locus::LocusOrdering;

use datafusion::{error::Result, prelude::*};

/// The reference combiner's ordering, shared by every reference combiner formulation.
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
        .read(ctx, &ScanShape::Flat)
        .await?
        .sort(query_ordering.sort_expressions())
}
