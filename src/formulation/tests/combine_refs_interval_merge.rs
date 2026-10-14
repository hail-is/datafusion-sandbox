//! Interval-merge: one merge per locus interval, each over every sample's scan filtered to the
//! interval, held by the sink above the union of intervals.
//!
//! `merge_tree` holds the plan's shape under each sink, and `rows` holds that merging by locus
//! interval returns, and writes, the union formulation's rows.

mod merge_tree;
mod rows;

use crate::formulation::combine_refs_interval_merge;
use crate::locus::SplitPoints;
use crate::ordered_frame::OrderedFrame;
use crate::stored::dataset::Dataset;
use crate::tests::{formulation_plans::PlanBuilder, support};

use datafusion::{error::Result, prelude::SessionContext};
use futures::{FutureExt, future::BoxFuture};

/// Split points cutting the fixture's eight loci into three intervals: `..chr01:3`, holding
/// chr01:1, chr01:2, and chr01:2; `chr01:3..chr02:2`, holding chr01:3, chr01:4, and chr02:1; and
/// `chr02:2..`, holding chr02:2 and chr02:3. The middle interval crosses the contig boundary.
const THREE_INTERVALS: &str = "1:3,2:2";

/// Split points whose middle interval, `chr01:5..chr02:1`, holds no fixture locus: it lies in the
/// gap between chr01:4 and chr02:1, both in file b, so every sample's scan keeps that file and
/// returns no rows from it.
const WITH_AN_EMPTY_INTERVAL: &str = "1:5,2:1";

/// The interval-merge formulation's plan builder over these split points.
#[derive(Clone)]
struct IntervalMerge(SplitPoints);

impl IntervalMerge {
    /// The plan builder over the comma-separated `split_points`. An empty string means no split
    /// points and therefore one locus interval.
    fn new(split_points: &str) -> Self {
        Self(support::split_points(split_points))
    }
}

impl PlanBuilder for IntervalMerge {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        combine_refs_interval_merge::plan(ctx, dataset, &self.0).boxed()
    }
}
