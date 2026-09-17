//! Test support about no dataset and no plan.

use crate::{formulation::Formulation, locus::SplitPoints, pipeline};
use datafusion::prelude::SessionConfig;
use std::num::NonZeroUsize;

/// The shared session settings, plus permission to split a file scan of any size across
/// `target_partitions` partitions wherever the plan lets the optimizer do so.
#[must_use]
pub(super) fn hostile_config(target_partitions: usize) -> SessionConfig {
    let mut config = pipeline::session_config().with_target_partitions(target_partitions);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    config
}

/// The grouped-merge formulation with `groups` sample groups.
///
/// # Panics
///
/// Panics if `groups` is zero.
#[must_use]
pub(super) fn grouped_merge(groups: usize) -> Formulation {
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
pub(super) fn interval_merge(split_points: &str) -> Formulation {
    let split_points = if split_points.is_empty() {
        SplitPoints::new(Vec::new())
    } else {
        split_points.parse()
    }
    .expect("test split points are valid");
    Formulation::CombineRefsIntervalMerge { split_points }
}
