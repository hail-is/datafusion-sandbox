//! Replay of shadow probes over a grid of stopping rule settings.
//!
//! A replay judges a recorded shadow probe again, from its recorded progress samples, as the
//! stopping rule would have judged it had it run with other settings. [`replay`] computes a run's
//! tightness checks once per pair of batch duration and window groups, and then finds the stop of
//! every combination of precision, consecutive checks and minimum duration over that pair's
//! checks, so a whole [`Grid`] takes about as long as its pairs. The poll period and the maximum
//! duration are always the run's own.
//!
//! This module is pure and in memory: its input is one shadow probe's settings, first partition
//! end and progress samples, and its output the rows of the three replay tables, which
//! [`crate::metrics_directory`] reads runs for and writes. The tables are:
//!
//! - **checks**: one row per tightness check per pair;
//! - **replay baselines**: one row per pair, with the end-of-run decision under it and when the
//!   probe would have been capped;
//! - **replays**: one row per combination, with when, and with what estimate, a shadow probe with
//!   it would first have stopped steady, and how a probe with it would have ended.

use crate::{
    run_metrics::{duration_ns, to_u64},
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason, TightnessCheck},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    error::Result,
};
use std::{num::NonZeroU32, sync::Arc, time::Duration};

/// The settings a replay judges each run under: every combination of one value of each.
#[derive(Clone, Debug, PartialEq)]
pub struct Grid {
    pub batch_durations: Vec<Duration>,
    pub window_groups: Vec<u32>,
    pub precisions: Vec<f64>,
    pub consecutive_checks: Vec<NonZeroU32>,
    pub min_durations: Vec<Duration>,
}

impl Default for Grid {
    /// The grid the `replay` command judges runs under: 576 combinations over 16 pairs, holding
    /// the default probe settings.
    fn default() -> Self {
        Self {
            batch_durations: [500, 1_000, 2_000, 5_000]
                .into_iter()
                .map(Duration::from_millis)
                .collect(),
            window_groups: vec![5, 10, 20, 40],
            precisions: vec![0.01, 0.02, 0.05],
            consecutive_checks: [1, 3, 5, 10]
                .into_iter()
                .filter_map(NonZeroU32::new)
                .collect(),
            min_durations: [10, 20, 60].into_iter().map(Duration::from_secs).collect(),
        }
    }
}

/// A recorded shadow probe, as a replay needs it.
#[derive(Clone, Debug, PartialEq)]
pub struct ShadowRun {
    pub run_id: String,
    /// The settings it was recorded with.
    pub settings: ProbeSettings,
    /// The elapsed nanoseconds of the first sample that showed a finished partition, if one did.
    pub first_partition_end_ns: Option<u64>,
    /// Its progress samples, in the order taken.
    pub samples: Vec<ProgressSample>,
}

/// A shadow probe judged again under every combination of a grid.
#[derive(Clone, Debug, PartialEq)]
pub struct Replay {
    pub run_id: String,
    /// The elapsed nanoseconds of the first sample at or past the recorded maximum duration, at
    /// which a probe would have been capped; `None` if no sample reached it.
    pub capped_at_ns: Option<u64>,
    /// One per pair of batch duration and window groups the run can be judged under, in the
    /// grid's order.
    pub pairs: Vec<PairReplay>,
}

/// A shadow probe judged again under one pair of batch duration and window groups.
#[derive(Clone, Debug, PartialEq)]
pub struct PairReplay {
    /// The recorded settings, with the pair's batch duration and window groups.
    pub settings: ProbeSettings,
    /// The tightness checks under the pair.
    pub checks: Vec<TightnessCheck>,
    /// The end-of-run decision under the pair, as the run record holds it; `None` if the rule
    /// makes none over every sample.
    pub baseline: Option<Decision>,
    /// One per combination of precision, consecutive checks and minimum duration, in the grid's
    /// order.
    pub combinations: Vec<CombinationReplay>,
}

/// A shadow probe judged again under one combination of the grid.
#[derive(Clone, Debug, PartialEq)]
pub struct CombinationReplay {
    /// The recorded settings, with the combination's five.
    pub settings: ProbeSettings,
    /// The tightness check at which a shadow probe with the combination would first have stopped
    /// steady, whatever the maximum duration; `None` if the rule never settled.
    pub would_stop: Option<TightnessCheck>,
    /// How a probe with the combination would have ended.
    pub probe_stop_reason: StopReason,
}

/// `run` judged again under every combination of `grid`. A pair whose batch duration is shorter
/// than the run's poll period is left out: its batches would rest on samples never taken.
#[must_use]
pub fn replay(grid: &Grid, run: &ShadowRun) -> Replay {
    let capped_at_ns = throughput_probe::capped_at_ns(&run.settings, &run.samples);
    let pairs = grid
        .batch_durations
        .iter()
        .filter(|&&batch_duration| batch_duration >= run.settings.poll_period)
        .flat_map(|&batch_duration| {
            grid.window_groups
                .iter()
                .map(move |&window_groups| ProbeSettings {
                    batch_duration,
                    window_groups,
                    ..run.settings.clone()
                })
        })
        .map(|settings| {
            let checks = throughput_probe::tightness_checks(&settings, &run.samples);
            let combinations = combinations(grid, &settings)
                .map(|settings| {
                    let would_stop =
                        throughput_probe::stop(&settings, &checks, run.first_partition_end_ns)
                            .cloned();
                    let probe_stop_reason = throughput_probe::probe_stop_reason(
                        would_stop.as_ref().map(|check| check.elapsed_ns),
                        capped_at_ns,
                        run.first_partition_end_ns,
                    );
                    CombinationReplay {
                        settings,
                        would_stop,
                        probe_stop_reason,
                    }
                })
                .collect();
            PairReplay {
                baseline: throughput_probe::decide(
                    &settings,
                    &run.samples,
                    run.first_partition_end_ns,
                ),
                settings,
                checks,
                combinations,
            }
        })
        .collect();
    Replay {
        run_id: run.run_id.clone(),
        capped_at_ns,
        pairs,
    }
}

/// `pair` with each combination of the grid's precision, consecutive checks and minimum duration.
fn combinations<'a>(
    grid: &'a Grid,
    pair: &'a ProbeSettings,
) -> impl Iterator<Item = ProbeSettings> + 'a {
    grid.precisions.iter().flat_map(move |&precision| {
        grid.consecutive_checks
            .iter()
            .flat_map(move |&consecutive_checks| {
                grid.min_durations
                    .iter()
                    .map(move |&min_duration| ProbeSettings {
                        precision,
                        consecutive_checks,
                        min_duration,
                        ..pair.clone()
                    })
            })
    })
}

/// The schema of the checks table: one row per tightness check of a run under each pair.
#[must_use]
pub fn checks_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("batch_duration_ns", DataType::UInt64, false),
        Field::new("window_groups", DataType::UInt64, false),
        Field::new("check_index", DataType::UInt64, false),
        Field::new("sample_index", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("warmup_end_ns", DataType::UInt64, true),
        Field::new("steady_state_throughput", DataType::Float64, true),
        Field::new("relative_half_width", DataType::Float64, true),
    ]))
}

/// The schema of the replay baselines table: one row per pair a run was judged under.
#[must_use]
pub fn replay_baselines_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("batch_duration_ns", DataType::UInt64, false),
        Field::new("window_groups", DataType::UInt64, false),
        Field::new("steady_state_throughput", DataType::Float64, true),
        Field::new("relative_half_width", DataType::Float64, true),
        Field::new("warmup_end_ns", DataType::UInt64, true),
        Field::new("window_end_ns", DataType::UInt64, true),
        Field::new("capped_at_ns", DataType::UInt64, true),
    ]))
}

/// The schema of the replays table: one row per combination a run was judged under.
#[must_use]
pub fn replays_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("batch_duration_ns", DataType::UInt64, false),
        Field::new("window_groups", DataType::UInt64, false),
        Field::new("precision", DataType::Float64, false),
        Field::new("consecutive_checks", DataType::UInt64, false),
        Field::new("min_duration_ns", DataType::UInt64, false),
        Field::new("would_stop_ns", DataType::UInt64, true),
        Field::new("would_be_steady_state_throughput", DataType::Float64, true),
        Field::new("would_be_relative_half_width", DataType::Float64, true),
        Field::new("would_be_warmup_end_ns", DataType::UInt64, true),
        Field::new("probe_stop_reason", DataType::Utf8, false),
    ]))
}

impl Replay {
    /// The batch of the checks table holding the replay's tightness checks.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be assembled.
    pub fn checks_batch(&self) -> Result<RecordBatch> {
        let rows: Vec<(&PairReplay, usize, &TightnessCheck)> = self
            .pairs
            .iter()
            .flat_map(|pair| {
                pair.checks
                    .iter()
                    .enumerate()
                    .map(move |(index, check)| (pair, index, check))
            })
            .collect();
        let mut columns = pair_columns(&self.run_id, &rows, |(pair, ..)| &pair.settings);
        columns.extend([
            u64_column(&rows, |&(_, index, _)| Some(to_u64(index))),
            u64_column(&rows, |(.., check)| Some(to_u64(check.sample_index))),
            u64_column(&rows, |(.., check)| Some(check.elapsed_ns)),
            u64_column(&rows, |(.., check)| check.warmup_end_ns),
            f64_column(&rows, |(.., check)| check.steady_state_throughput),
            f64_column(&rows, |(.., check)| check.relative_half_width),
        ]);
        Ok(RecordBatch::try_new(checks_schema(), columns)?)
    }

    /// The batch of the replay baselines table holding the replay's end-of-run decisions.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be assembled.
    pub fn replay_baselines_batch(&self) -> Result<RecordBatch> {
        let pairs = &self.pairs;
        let mut columns = pair_columns(&self.run_id, pairs, |pair| &pair.settings);
        columns.extend([
            f64_column(pairs, |pair| {
                pair.baseline.as_ref()?.steady_state_throughput
            }),
            f64_column(pairs, |pair| pair.baseline.as_ref()?.relative_half_width),
            u64_column(pairs, |pair| pair.baseline.as_ref()?.warmup_end_ns),
            u64_column(pairs, |pair| Some(pair.baseline.as_ref()?.window_end_ns)),
            u64_column(pairs, |_| self.capped_at_ns),
        ]);
        Ok(RecordBatch::try_new(replay_baselines_schema(), columns)?)
    }

    /// The batch of the replays table holding the replay's combinations.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be assembled.
    pub fn replays_batch(&self) -> Result<RecordBatch> {
        let rows: Vec<&CombinationReplay> = self
            .pairs
            .iter()
            .flat_map(|pair| &pair.combinations)
            .collect();
        let mut columns = pair_columns(&self.run_id, &rows, |row| &row.settings);
        columns.extend([
            f64_column(&rows, |row| Some(row.settings.precision)),
            u64_column(&rows, |row| {
                Some(u64::from(row.settings.consecutive_checks.get()))
            }),
            u64_column(&rows, |row| Some(duration_ns(row.settings.min_duration))),
            u64_column(&rows, |row| Some(row.would_stop.as_ref()?.elapsed_ns)),
            f64_column(&rows, |row| {
                row.would_stop.as_ref()?.steady_state_throughput
            }),
            f64_column(&rows, |row| row.would_stop.as_ref()?.relative_half_width),
            u64_column(&rows, |row| row.would_stop.as_ref()?.warmup_end_ns),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.probe_stop_reason.name()),
            )),
        ]);
        Ok(RecordBatch::try_new(replays_schema(), columns)?)
    }
}

/// The columns every replay table starts with, naming the run and the pair each of `rows` was
/// judged under: `run_id`, `batch_duration_ns` and `window_groups`.
fn pair_columns<T>(
    run_id: &str,
    rows: &[T],
    settings: impl Fn(&T) -> &ProbeSettings,
) -> Vec<ArrayRef> {
    vec![
        Arc::new(StringArray::from(vec![run_id; rows.len()])),
        u64_column(rows, |row| Some(duration_ns(settings(row).batch_duration))),
        u64_column(rows, |row| Some(u64::from(settings(row).window_groups))),
    ]
}

/// The `UInt64` column holding `cell` of each of `rows`, null where it is `None`.
fn u64_column<T>(rows: &[T], cell: impl Fn(&T) -> Option<u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter(rows.iter().map(cell)))
}

/// The `Float64` column holding `cell` of each of `rows`, null where it is `None`.
fn f64_column<T>(rows: &[T], cell: impl Fn(&T) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from_iter(rows.iter().map(cell)))
}
