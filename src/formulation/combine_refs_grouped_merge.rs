use super::combine_refs_union::required_ordering;
use crate::locus::StoredOrdering;
use crate::stored::dataset::{Dataset, union_or_single};

use datafusion::{error::Result, prelude::*};
use std::num::NonZeroUsize;

/// Splits `samples` into at most `groups` contiguous, count-balanced sample groups, with larger
/// groups first. A group count above the sample count clamps to one sample per group.
#[must_use]
pub fn sample_groups(samples: &[String], groups: NonZeroUsize) -> Vec<&[String]> {
    // Dataset sample sets are nonempty, so the clamped count keeps both divisions defined.
    let groups = groups.get().min(samples.len());
    let quotient = samples.len().checked_div(groups).unwrap_or(0);
    let remainder = samples.len().checked_rem(groups).unwrap_or(0);
    let mut rest = samples;
    (0..groups)
        .map(|index| {
            let size = quotient.saturating_add(usize::from(index < remainder));
            let (group, tail) = rest.split_at(size);
            rest = tail;
            group
        })
        .collect()
}

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
///
/// # Errors
///
/// Returns an error if the dataset cannot satisfy the reference combiner's ordering, a sample
/// group cannot be read, or `DataFusion` cannot build the plan.
pub async fn plan(
    ctx: &SessionContext,
    dataset: &Dataset,
    groups: NonZeroUsize,
) -> Result<(DataFrame, StoredOrdering)> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let groups = sample_groups(dataset.sample_set(), groups);
    let mut plans = Vec::with_capacity(groups.len());
    for group in groups {
        let frame = dataset
            .restrict_to(group)?
            .read(ctx)
            .await?
            .sort(ordering.sort_expressions())?;
        plans.push(frame.into_unoptimized_plan());
    }
    Ok((union_or_single(ctx, plans)?, ordering))
}
