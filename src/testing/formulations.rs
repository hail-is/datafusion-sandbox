//! Formulations and split points built from compact test notation.

use crate::{formulation::Formulation, locus::SplitPoints};
use std::num::NonZeroUsize;

/// The grouped-merge formulation with `groups` input groups.
///
/// # Panics
///
/// Panics if `groups` is zero.
#[must_use]
pub(crate) fn grouped_merge(groups: usize) -> Formulation {
    Formulation::CombineRefsGroupedMerge {
        groups: NonZeroUsize::new(groups).expect("a grouped merge needs at least one group"),
    }
}

/// The interval-merge formulation with the comma-separated `split_points`.
///
/// An empty string means no split points and therefore one locus interval.
///
/// # Panics
///
/// Panics if `split_points` is not empty and is not a valid, strictly increasing list.
#[must_use]
pub(crate) fn interval_merge(split_points: &str) -> Formulation {
    Formulation::CombineRefsIntervalMerge {
        split_points: self::split_points(split_points),
    }
}

/// The comma-separated `split_points`. An empty string means no split points.
///
/// # Panics
///
/// Panics if `split_points` is not empty and is not a valid, strictly increasing list.
#[must_use]
pub(crate) fn split_points(split_points: &str) -> SplitPoints {
    if split_points.is_empty() {
        SplitPoints::new(Vec::new())
    } else {
        split_points.parse()
    }
    .expect("test split points are valid")
}
