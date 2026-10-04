use crate::locus::{RowOrdering, StoredOrdering};
use crate::stored::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

/// The least row ordering of the reference combiner, shared by every reference combiner
/// formulation.
pub fn required_ordering() -> RowOrdering {
    RowOrdering::locus()
}

/// Builds the union-of-input-table-scans formulation, sorted by the dataset's whole row ordering.
///
/// Many `DataSourceExec`s feed a `UnionExec`, which feeds a
/// `SortPreservingMergeExec` with one partition per input table.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<(DataFrame, StoredOrdering)> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let frame = dataset.read(ctx).await?.sort(ordering.sort_expressions())?;
    Ok((frame, ordering))
}
