use super::combine_refs_union;
use crate::dataset::{Dataset, union_or_single};
use crate::locus::{SplitPoints, StoredOrdering};

use datafusion::{error::Result, prelude::*};

/// Builds the interval-merge formulation: merge every sample within each locus interval.
///
/// The frame is the union formulation's frame filtered by each of the `j` locus intervals the
/// split points define, the `j` branches unioned. Each interval's filter reaches every sample's
/// scan, which prunes its files by it, so a branch's `DataSourceExec`s feed a `UnionExec` under
/// a `SortPreservingMergeExec` that merges only that interval's rows, and the `j` merges feed the
/// outer `UnionExec` as one partition each.
///
/// Nothing above the outer union orders the intervals; the sink every action runs the frame into
/// does. A partitioned file sink requires the ordering of each partition and writes each interval
/// merge to its own file, so the `j` merges run alongside each other (ADR 0015). A single-partition
/// sink requires the ordering of the whole and gets one more merge over the interval merges, so
/// collect and explain see the rows in global locus order (ADR 0014).
///
/// With no split points the one interval has no filter and the plan is the union formulation's.
pub async fn plan(
    ctx: &SessionContext,
    dataset: &Dataset,
    split_points: &SplitPoints,
) -> Result<(DataFrame, StoredOrdering)> {
    let (union, ordering) = combine_refs_union::plan(ctx, dataset).await?;
    let representation = dataset.locus_representation();
    let mut branches = Vec::new();
    for interval in split_points.intervals() {
        let branch = match interval.filter(representation) {
            Some(filter) => union.clone().filter(filter)?,
            None => union.clone(),
        };
        branches.push(branch.into_unoptimized_plan());
    }
    Ok((union_or_single(ctx, branches)?, ordering))
}
