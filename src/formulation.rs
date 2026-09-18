//! The supported combiner formulations and their shared plan-building helpers.

mod combine_alleles;
mod combine_refs_grouped_merge;
mod combine_refs_interval_merge;
mod combine_refs_union;

#[cfg(test)]
mod tests;

use crate::{
    dataset::Dataset,
    locus::{LocusOrdering, SplitPoints},
    ordered_frame::{OrderedFrame, OutputLayout},
};

use datafusion::{error::Result, prelude::*};
use std::{fmt, num::NonZeroUsize};

/// A supported way to build one of the combiners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Formulation {
    CombineAllelesUnion,
    CombineRefsUnion,
    /// The reference combiner merging each of `groups` sample groups, then merging the groups.
    CombineRefsGroupedMerge {
        groups: NonZeroUsize,
    },
    /// The reference combiner merging every sample within each of the locus intervals
    /// `split_points` define, one file per interval.
    CombineRefsIntervalMerge {
        split_points: SplitPoints,
    },
}

impl fmt::Display for Formulation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CombineAllelesUnion | Self::CombineRefsUnion => formatter.write_str("union"),
            Self::CombineRefsGroupedMerge { .. } => formatter.write_str("grouped-merge"),
            Self::CombineRefsIntervalMerge { .. } => formatter.write_str("interval-merge"),
        }
    }
}

impl Formulation {
    #[must_use]
    pub fn required_ordering(&self) -> LocusOrdering {
        match self {
            Self::CombineAllelesUnion => combine_alleles::required_ordering(),
            Self::CombineRefsUnion
            | Self::CombineRefsGroupedMerge { .. }
            | Self::CombineRefsIntervalMerge { .. } => combine_refs_union::required_ordering(),
        }
    }

    /// The sample group count of a grouped merge; `None` for every other formulation.
    #[must_use]
    pub const fn groups(&self) -> Option<NonZeroUsize> {
        match self {
            Self::CombineRefsGroupedMerge { groups } => Some(*groups),
            Self::CombineAllelesUnion
            | Self::CombineRefsUnion
            | Self::CombineRefsIntervalMerge { .. } => None,
        }
    }

    /// The split points of an interval merge; `None` for every other formulation.
    #[must_use]
    pub const fn split_points(&self) -> Option<&SplitPoints> {
        match self {
            Self::CombineRefsIntervalMerge { split_points } => Some(split_points),
            Self::CombineAllelesUnion
            | Self::CombineRefsUnion
            | Self::CombineRefsGroupedMerge { .. } => None,
        }
    }

    /// How a write of this formulation's rows lays them out: one file, or one file per partition
    /// of its frame, which for interval-merge is one per locus interval.
    #[must_use]
    pub const fn output_layout(&self) -> OutputLayout {
        match self {
            Self::CombineAllelesUnion
            | Self::CombineRefsUnion
            | Self::CombineRefsGroupedMerge { .. } => OutputLayout::SingleFile,
            Self::CombineRefsIntervalMerge { .. } => OutputLayout::FilePerPartition,
        }
    }

    /// Builds this formulation's plan over `dataset`.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset cannot satisfy the formulation's required ordering or if
    /// `DataFusion` cannot build the plan.
    pub async fn plan(&self, ctx: &SessionContext, dataset: &Dataset) -> Result<OrderedFrame> {
        let (frame, ordering) = match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
            Self::CombineRefsGroupedMerge { groups } => {
                combine_refs_grouped_merge::plan(ctx, dataset, *groups).await
            }
            Self::CombineRefsIntervalMerge { split_points } => {
                combine_refs_interval_merge::plan(ctx, dataset, split_points).await
            }
        }?;
        Ok(OrderedFrame {
            frame,
            ordering,
            layout: self.output_layout(),
        })
    }
}
