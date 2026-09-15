use super::combine_refs_union::required_ordering;
use crate::dataset::{Dataset, ScanShape};

use datafusion::{error::Result, prelude::*};
use std::num::NonZeroUsize;

/// Builds the grouped-merge formulation: merge each sample group, then merge the groups.
///
/// Each group's `DataSourceExec`s feed a `UnionExec` under a `SortPreservingMergeExec`, and
/// those merges feed a second `UnionExec` under the final `SortPreservingMergeExec`. A merge
/// pulls every input on its own task, so the group merges run alongside each other and the
/// final merge.
///
/// The frame ends in the union of groups with no sort above it. The final merge comes from the
/// sink every action runs the frame into: its ordering requirement over the union of ordered
/// groups becomes the outer merge, and the group sorts become the inner ones. A final logical
/// sort here would instead have the optimizer replace every group merge with a coalesce and
/// delete the group sorts as redundant beneath it, leaving one flat merge. See ADR 0014.
///
/// With one group, or one sample per group, the plan is the union formulation's.
pub async fn plan(
    ctx: &SessionContext,
    dataset: &Dataset,
    groups: NonZeroUsize,
) -> Result<DataFrame> {
    let query_ordering = dataset.query_ordering(&required_ordering())?;
    let shape = ScanShape::SampleGroups {
        groups,
        ordering: query_ordering,
    };
    dataset.read(ctx, &shape).await
}
