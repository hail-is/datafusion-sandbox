//! Test support about no dataset and no plan.

use crate::{formulation::Formulation, locus::SplitPoints, pipeline};
use datafusion::{
    arrow::{
        array::{TimestampNanosecondArray, UInt64Array},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    prelude::SessionConfig,
};
use std::num::NonZeroUsize;

/// The values of the string column `name`, rendered, a null rendering as the empty string. A
/// string column read back from Parquet is a view array, so this renders rather than downcasts.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or a value cannot be rendered.
#[must_use]
pub(super) fn string_values(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    (0..column.len())
        .map(|row| array_value_to_string(column, row).expect("a rendered cell"))
        .collect()
}

/// The values of the `UInt64` column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a `UInt64` column.
#[must_use]
pub(super) fn u64_values(batch: &RecordBatch, name: &str) -> Vec<Option<u64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("{name} is not a UInt64 column: {column:?}"))
        .iter()
        .collect()
}

/// The values of the nanosecond timestamp column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a nanosecond timestamp column.
#[must_use]
pub(super) fn timestamp_values(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap_or_else(|| panic!("{name} is not a nanosecond timestamp column: {column:?}"))
        .iter()
        .collect()
}

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
