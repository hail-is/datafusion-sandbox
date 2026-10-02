//! Test support about no dataset and no plan.

use crate::{
    formulation::Formulation,
    locus::SplitPoints,
    pipeline,
    run_metrics::{FormulationRecord, RunRecord, WriteRecord},
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample},
};
use datafusion::{
    arrow::{
        array::{Float64Array, TimestampNanosecondArray, UInt64Array},
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

/// The values of the `Float64` column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a `Float64` column.
#[must_use]
pub(super) fn f64_values(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{name} is not a Float64 column: {column:?}"))
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

/// The grouped-merge formulation with `groups` input groups.
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
        write: Some(WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("snappy".to_string()),
        }),
        threads: 1,
        input_tables: 4,
        samples: 4,
        rows_written: 32,
        run_ns: 2_000,
        execute_ns: 1_000,
        peak_rss_bytes: 1 << 20,
        probe: None,
    }
}

/// A made-up series of progress samples: a ramp of `ramp` seconds, then a rate of 1,000 rows/s
/// with relative noise up to `noise`, polled every 100 ms with up to 50 ms of jitter, for 25 s.
#[expect(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "made-up series are far too short to overflow, and their rows need no precision"
)]
pub(super) fn noisy_series(seed: u64, ramp: u64, noise: f64) -> Vec<ProgressSample> {
    const MS: u64 = 1_000_000;
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut uniform = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 11) as f64 / (1_u64 << 53) as f64
    };
    let (mut elapsed_ns, mut rows) = (0_u64, 0.0_f64);
    let mut samples = vec![ProgressSample {
        elapsed_ns: 0,
        rows: 0,
    }];
    while elapsed_ns < 25_000 * MS {
        let gap_ns = 100 * MS + (uniform() * 50.0) as u64 * MS;
        let seconds = elapsed_ns / (1_000 * MS);
        let level = if seconds < ramp {
            (seconds + 1) as f64 / (ramp + 1) as f64
        } else {
            1.0
        };
        let rate = 1_000.0 * level * noise.mul_add(2.0_f64.mul_add(uniform(), -1.0), 1.0);
        elapsed_ns += gap_ns;
        rows += rate * gap_ns as f64 / 1e9;
        samples.push(ProgressSample {
            elapsed_ns,
            rows: rows as u64,
        });
    }
    samples
}

/// The would-stop a shadow probe with `settings` records over `samples`: the first decision
/// [`throughput_probe::would_stop`] makes, judging after every sample as the probe does.
/// `first_partition_end_ns` reaches the rule once a sample has shown it.
pub(super) fn recorded_would_stop(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> Option<Decision> {
    after_each_sample(samples, first_partition_end_ns, |seen, end_ns| {
        throughput_probe::would_stop(settings, seen, end_ns)
    })
}

/// The decision a probe with `settings` stops with over `samples`: the first decision
/// [`throughput_probe::decide`] makes, judging after every sample as the probe does.
/// `first_partition_end_ns` reaches the rule once a sample has shown it.
pub(super) fn first_decision(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> Option<Decision> {
    after_each_sample(samples, first_partition_end_ns, |seen, end_ns| {
        throughput_probe::decide(settings, seen, end_ns)
    })
}

/// The first decision `judge` makes over the samples seen after each of `samples`, given the
/// first partition end once the latest sample seen is at or past it.
fn after_each_sample(
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
    judge: impl Fn(&[ProgressSample], Option<u64>) -> Option<Decision>,
) -> Option<Decision> {
    (1..=samples.len()).find_map(|taken| {
        let seen = samples.get(..taken)?;
        let latest_ns = seen.last()?.elapsed_ns;
        judge(
            seen,
            first_partition_end_ns.filter(|&end_ns| end_ns <= latest_ns),
        )
    })
}
