use crate::locus::RowOrdering;
use crate::ordered_frame::{OrderedFrame, OutputLayout};
use crate::stored::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

/// The least row ordering of the reference combiner, shared by every reference combiner
/// formulation.
pub(super) fn required_ordering() -> RowOrdering {
    RowOrdering::locus()
}

/// How a write of the union formulation's rows lays them out: one file.
pub(super) const fn output_layout() -> OutputLayout {
    OutputLayout::SingleFile
}

/// Builds the union-of-input-table-scans formulation, sorted by the dataset's whole row ordering.
///
/// Many `DataSourceExec`s feed a `UnionExec`, which feeds a
/// `SortPreservingMergeExec` with one partition per input table.
pub(super) async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<OrderedFrame> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let frame = dataset.read(ctx).await?.sort(ordering.sort_expressions())?;
    Ok(OrderedFrame {
        frame,
        ordering,
        layout: output_layout(),
    })
}
