//! The supported combiner formulations, each dispatching to the plan builder of its own child
//! module.

mod combine_alleles;
mod combine_refs_grouped_merge;
mod combine_refs_interval_merge;
mod combine_refs_union;

#[cfg(test)]
mod tests;

use crate::{
    locus::{RowOrdering, SplitPoints},
    ordered_frame::{OrderedFrame, OutputLayout},
    stored::dataset::Dataset,
};

use datafusion::{error::Result, prelude::*};
use std::{fmt, num::NonZeroUsize};

/// A supported way to build one of the combiners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Formulation {
    CombineAllelesUnion,
    CombineRefsUnion,
    /// The reference combiner merging each of `groups` input groups, then merging the groups.
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
    /// The least row ordering this formulation can merge under. It plans under its dataset's
    /// whole row ordering, which must start with this one.
    #[must_use]
    pub fn required_ordering(&self) -> RowOrdering {
        match self {
            Self::CombineAllelesUnion => combine_alleles::required_ordering(),
            Self::CombineRefsUnion
            | Self::CombineRefsGroupedMerge { .. }
            | Self::CombineRefsIntervalMerge { .. } => combine_refs_union::required_ordering(),
        }
    }

    /// How a write of this formulation's rows lays them out: one file, or one file per partition
    /// of its frame, which for interval-merge is one per locus interval.
    #[must_use]
    pub const fn output_layout(&self) -> OutputLayout {
        match self {
            Self::CombineAllelesUnion => combine_alleles::output_layout(),
            Self::CombineRefsUnion => combine_refs_union::output_layout(),
            Self::CombineRefsGroupedMerge { .. } => combine_refs_grouped_merge::output_layout(),
            Self::CombineRefsIntervalMerge { .. } => combine_refs_interval_merge::output_layout(),
        }
    }

    /// Whether a write of this formulation's rows is a multi-sample input table to a later run, and
    /// so writes a sample annotation table beside its data: true for the reference combiner.
    #[must_use]
    pub const fn writes_sample_annotation_table(&self) -> bool {
        match self {
            Self::CombineAllelesUnion => false,
            Self::CombineRefsUnion
            | Self::CombineRefsGroupedMerge { .. }
            | Self::CombineRefsIntervalMerge { .. } => true,
        }
    }

    /// Builds this formulation's plan over `dataset`.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset cannot satisfy the formulation's required ordering or if
    /// `DataFusion` cannot build the plan.
    pub async fn plan(&self, ctx: &SessionContext, dataset: &Dataset) -> Result<OrderedFrame> {
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
            Self::CombineRefsGroupedMerge { groups } => {
                combine_refs_grouped_merge::plan(ctx, dataset, *groups).await
            }
            Self::CombineRefsIntervalMerge { split_points } => {
                combine_refs_interval_merge::plan(ctx, dataset, split_points).await
            }
        }
    }
}
