use super::combine_refs_union::required_ordering;
use crate::locus::StoredOrdering;
use crate::stored::dataset::{Dataset, InputTable, union_or_single};

use datafusion::{error::Result, prelude::*};
use std::num::NonZeroUsize;

/// Splits `input_tables` into at most `groups` contiguous, count-balanced input groups, with
/// larger groups first. A group count above the input table count clamps to one input table per
/// group.
#[must_use]
pub fn input_groups(input_tables: &[InputTable], groups: NonZeroUsize) -> Vec<&[InputTable]> {
    // Datasets hold at least one input table, so the clamped count keeps both divisions defined.
    let groups = groups.get().min(input_tables.len());
    let quotient = input_tables.len().checked_div(groups).unwrap_or(0);
    let remainder = input_tables.len().checked_rem(groups).unwrap_or(0);
    let mut rest = input_tables;
    (0..groups)
        .map(|index| {
            let size = quotient.saturating_add(usize::from(index < remainder));
            let (group, tail) = rest.split_at(size);
            rest = tail;
            group
        })
        .collect()
}

/// Builds the grouped-merge formulation: merge each input group, then merge the groups.
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
/// With one group, or one input table per group, the plan is the union formulation's.
///
/// # Errors
///
/// Returns an error if the dataset cannot satisfy the reference combiner's ordering, an input
/// group cannot be read, or `DataFusion` cannot build the plan.
pub async fn plan(
    ctx: &SessionContext,
    dataset: &Dataset,
    groups: NonZeroUsize,
) -> Result<(DataFrame, StoredOrdering)> {
    let ordering = dataset.query_ordering(&required_ordering())?;
    let groups = input_groups(dataset.input_tables(), groups);
    let mut plans = Vec::with_capacity(groups.len());
    for group in groups {
        let names = group
            .iter()
            .map(|input_table| input_table.name().to_string())
            .collect::<Vec<_>>();
        let frame = dataset
            .restrict_to(&names)?
            .read(ctx)
            .await?
            .sort(ordering.sort_expressions())?;
        plans.push(frame.into_unoptimized_plan());
    }
    Ok((union_or_single(ctx, plans)?, ordering))
}
