//! Test support about no dataset and no plan.

use crate::{
    formulation::Formulation,
    locus::SplitPoints,
    pipeline,
    run_metrics::{FormulationRecord, RunRecord, WriteRecord},
};
use datafusion::{
    arrow::{
        array::{TimestampNanosecondArray, UInt64Array},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    prelude::SessionConfig,
};
use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};

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

/// The rows of the run metrics batch `batch` whose operator is `operator`, in order.
///
/// # Panics
///
/// Panics if `batch` has no `operator` column.
#[must_use]
pub(super) fn rows_of_operator(batch: &RecordBatch, operator: &str) -> Vec<usize> {
    string_values(batch, "operator")
        .iter()
        .enumerate()
        .filter(|(_, name)| *name == operator)
        .map(|(row, _)| row)
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

/// A run record of `run_id` whose other fields hold plausible settings and measurements of a
/// grouped-merge write, for tests that need a record but assert on none of its facts.
#[must_use]
pub(super) fn run_record(run_id: &str) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        started_at: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(1_700_000_000))
            .expect("a start time after the epoch"),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(2),
            split_points: None,
        },
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        write: WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("snappy".to_string()),
        },
        threads: 1,
        samples: 4,
        rows_written: 32,
        run_ns: 2_000,
        execute_ns: 1_000,
        peak_rss_bytes: 1 << 20,
    }
}
