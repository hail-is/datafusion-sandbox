use crate::locus::{LocusOrdering, StoredOrdering};
use crate::stored::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

/// The reference combiner's ordering, shared by every reference combiner formulation.
pub fn required_ordering() -> LocusOrdering {
    LocusOrdering::locus()
}

/// Builds the union-of-per-sample-scans formulation.
///
/// Many `DataSourceExec`s feed a `UnionExec`, which feeds a
/// `SortPreservingMergeExec` with one partition per input sample.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<(DataFrame, StoredOrdering)> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let frame = dataset.read(ctx).await?.sort(ordering.sort_expressions())?;
    Ok((frame, ordering))
}
