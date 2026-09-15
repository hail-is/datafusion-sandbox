use super::combine_refs_union;
use crate::dataset::Dataset;
use crate::locus::SplitPoints;

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{LogicalPlan, logical_plan::Union},
    prelude::*,
};
use std::sync::Arc;

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
) -> Result<DataFrame> {
    let union = combine_refs_union::plan(ctx, dataset).await?;
    let representation = dataset.locus_representation();
    let mut branches = Vec::new();
    for interval in split_points.intervals() {
        let branch = match interval.filter(representation) {
            Some(filter) => union.clone().filter(filter)?,
            None => union.clone(),
        };
        branches.push(branch.into_unoptimized_plan());
    }
    let plan = if branches.len() == 1 {
        branches.pop().ok_or_else(|| {
            DataFusionError::Internal("split points defined no locus interval".to_string())
        })?
    } else {
        LogicalPlan::Union(Union::try_new(
            branches.into_iter().map(Arc::new).collect(),
        )?)
    };
    Ok(DataFrame::new(ctx.state(), plan))
}
