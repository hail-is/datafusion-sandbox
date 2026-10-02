use crate::{
    replay::{self, Grid, Replay, ShadowRun},
    tests::support::{
        f64_values, first_decision, noisy_series, recorded_would_stop, string_values, u64_values,
    },
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason},
};

use std::{num::NonZeroU32, time::Duration};

const MS: u64 = 1_000_000;
const S: u64 = 1_000 * MS;

/// A shadow run over `samples` with the default settings but `poll_period` and `max_duration`.
fn shadow_run(
    samples: Vec<ProgressSample>,
    poll_period: Duration,
    max_duration: Duration,
    first_partition_end_ns: Option<u64>,
) -> ShadowRun {
    ShadowRun {
        run_id: "shadow".to_string(),
        settings: ProbeSettings {
            poll_period,
            max_duration,
            ..ProbeSettings::default()
        },
        first_partition_end_ns,
        samples,
    }
}

/// A grid small enough to judge prefix by prefix, whose stops fall within [`noisy_series`].
fn small_grid() -> Grid {
    Grid {
        batch_durations: vec![Duration::from_millis(300), Duration::from_secs(1)],
        window_groups: vec![2, 5],
        precisions: vec![0.05, 0.3],
        consecutive_checks: [1, 3].into_iter().filter_map(NonZeroU32::new).collect(),
        min_durations: vec![Duration::ZERO, Duration::from_secs(8)],
    }
}

/// Shadow runs that stop steady, are capped and complete under some of [`small_grid`]'s
/// combinations, one capped at the sample that showed its first partition end. A partition end
/// and a cap given by index are at that sample's time.
fn judged_runs() -> Vec<ShadowRun> {
    let ending = |seed, ramp, noise, end: Option<usize>, cap: usize| {
        let samples = noisy_series(seed, ramp, noise);
        let at = |index: usize| samples.get(index).map(|sample| sample.elapsed_ns);
        let (end_ns, cap_ns) = (end.and_then(at), at(cap).unwrap_or(u64::MAX));
        shadow_run(
            samples,
            Duration::from_millis(100),
            Duration::from_nanos(cap_ns),
            end_ns,
        )
    };
    vec![
        ending(1, 0, 0.0, Some(190), usize::MAX),
        ending(2, 4, 0.05, Some(150), 100),
        ending(3, 6, 0.3, Some(170), 80),
        ending(4, 3, 0.1, Some(80), 50),
        ending(5, 8, 0.02, None, 120),
        ending(6, 2, 0.05, Some(40), usize::MAX),
        ending(7, 1, 0.1, Some(70), 70),
    ]
}

#[test]
fn the_default_grid_judges_576_combinations_over_16_pairs() {
    let samples = noisy_series(1, 0, 0.0);
    let run = shadow_run(
        samples,
        Duration::from_millis(100),
        Duration::from_secs(300),
        None,
    );
    let replayed = replay::replay(&Grid::default(), &run);
    let defaults = ProbeSettings {
        max_duration: Duration::from_secs(300),
        ..ProbeSettings::default()
    };

    assert_eq!(replayed.pairs.len(), 16);
    assert!(
        replayed
            .pairs
            .iter()
            .all(|pair| pair.combinations.len() == 36)
    );
    assert!(
        replayed
            .pairs
            .iter()
            .flat_map(|pair| &pair.combinations)
            .any(|combination| combination.settings == defaults)
    );
}

/// A probe polled every 2 s has no probe batch of 0.5 or 1 s, so replay leaves out the pairs with
/// those batch durations, in every table.
#[test]
fn leaves_out_pairs_whose_batch_duration_is_shorter_than_the_poll_period() {
    let samples: Vec<ProgressSample> = (0..=60)
        .map(|index| ProgressSample {
            elapsed_ns: index * 2 * S,
            rows: index * 2_000,
        })
        .collect();
    let run = shadow_run(
        samples,
        Duration::from_secs(2),
        Duration::from_secs(300),
        None,
    );
    let replayed = replay::replay(&Grid::default(), &run);
    let batch_durations_ns = |batch: datafusion::arrow::record_batch::RecordBatch| {
        let mut durations: Vec<Option<u64>> = u64_values(&batch, "batch_duration_ns");
        durations.dedup();
        durations
    };

    assert_eq!(replayed.pairs.len(), 8);
    assert_eq!(
        batch_durations_ns(replayed.replay_baselines_batch().unwrap()),
        [Some(2 * S), Some(5 * S)]
    );
    assert_eq!(
        batch_durations_ns(replayed.checks_batch().unwrap()),
        [Some(2 * S), Some(5 * S)]
    );
    assert_eq!(
        batch_durations_ns(replayed.replays_batch().unwrap()),
        [Some(2 * S), Some(5 * S)]
    );
}

/// A replay says each combination ends as a probe deciding after every sample with it does, and
/// a steady one at its would-stop.
#[test]
fn the_probe_stop_reason_is_how_a_probe_deciding_after_every_sample_ends() {
    let mut reasons = Vec::new();
    for run in judged_runs() {
        let replayed = replay::replay(&small_grid(), &run);
        for combination in replayed.pairs.iter().flat_map(|pair| &pair.combinations) {
            let decided = first_decision(
                &combination.settings,
                &run.samples,
                run.first_partition_end_ns,
            )
            .unwrap();
            let steady_at = combination
                .would_stop
                .as_ref()
                .map(|check| check.elapsed_ns);

            assert_eq!(
                combination.probe_stop_reason, decided.stop_reason,
                "{combination:?}"
            );
            if decided.stop_reason == StopReason::Steady {
                assert_eq!(steady_at, Some(decided.window_end_ns), "{combination:?}");
            }
            reasons.push(decided.stop_reason);
        }
    }
    for reason in [
        StopReason::Steady,
        StopReason::Capped,
        StopReason::Completed,
    ] {
        assert!(reasons.contains(&reason), "{reason:?}");
    }
}

/// A combination's would-stop is the first decision a shadow probe with it records, deciding
/// after every sample.
#[test]
fn each_combination_would_stop_where_a_shadow_probe_with_it_would() {
    let mut stops = 0;
    for run in judged_runs() {
        let replayed = replay::replay(&small_grid(), &run);
        for combination in replayed.pairs.iter().flat_map(|pair| &pair.combinations) {
            let recorded = recorded_would_stop(
                &combination.settings,
                &run.samples,
                run.first_partition_end_ns,
            );
            let replayed = combination.would_stop.as_ref().map(|check| {
                (
                    check.elapsed_ns,
                    check.steady_state_throughput,
                    check.relative_half_width,
                    check.warmup_end_ns,
                )
            });

            stops += usize::from(recorded.is_some());
            assert_eq!(
                replayed,
                recorded.map(|decision| (
                    decision.window_end_ns,
                    decision.steady_state_throughput,
                    decision.relative_half_width,
                    decision.warmup_end_ns,
                )),
                "{combination:?}"
            );
        }
    }
    assert!(stops > 0);
}

#[test]
fn each_pair_s_baseline_is_the_end_of_run_decision_under_it() {
    for run in judged_runs() {
        let replayed = replay::replay(&small_grid(), &run);
        let expected: Vec<Option<Decision>> = small_grid()
            .batch_durations
            .into_iter()
            .flat_map(|batch_duration| {
                small_grid()
                    .window_groups
                    .into_iter()
                    .map(move |window_groups| (batch_duration, window_groups))
            })
            .map(|(batch_duration, window_groups)| {
                let settings = ProbeSettings {
                    batch_duration,
                    window_groups,
                    ..run.settings.clone()
                };
                throughput_probe::decide(&settings, &run.samples, run.first_partition_end_ns)
            })
            .collect();

        assert_eq!(
            replayed
                .pairs
                .iter()
                .map(|pair| pair.baseline.clone())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

/// One run's replay, its first pair's combinations stopping where the replay says.
fn replayed_run() -> (ShadowRun, Replay) {
    let run = judged_runs().swap_remove(1);
    let replayed = replay::replay(&small_grid(), &run);
    (run, replayed)
}

/// The checks table holds every pair's tightness checks, in order, as the replay found them.
#[test]
fn the_checks_table_holds_each_pair_s_checks_in_order() {
    let (_, replayed) = replayed_run();
    let batch = replayed.checks_batch().unwrap();
    let checks: Vec<_> = replayed
        .pairs
        .iter()
        .flat_map(|pair| {
            pair.checks
                .iter()
                .enumerate()
                .map(move |check| (pair, check))
        })
        .collect();

    assert_eq!(batch.schema(), replay::checks_schema());
    assert!(
        string_values(&batch, "run_id")
            .iter()
            .all(|id| id == "shadow")
    );
    assert_eq!(
        u64_values(&batch, "window_groups"),
        checks
            .iter()
            .map(|(pair, _)| Some(u64::from(pair.settings.window_groups)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "check_index"),
        checks
            .iter()
            .map(|(_, (index, _))| Some(u64::try_from(*index).unwrap()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "sample_index"),
        checks
            .iter()
            .map(|(_, (_, check))| Some(u64::try_from(check.sample_index).unwrap()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "warmup_end_ns"),
        checks
            .iter()
            .map(|(_, (_, check))| check.warmup_end_ns)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        f64_values(&batch, "relative_half_width"),
        checks
            .iter()
            .map(|(_, (_, check))| check.relative_half_width)
            .collect::<Vec<_>>()
    );
}

#[test]
fn the_replay_baselines_table_holds_each_pair_s_end_of_run_decision_and_the_cap() {
    let (run, replayed) = replayed_run();
    let batch = replayed.replay_baselines_batch().unwrap();
    let baselines: Vec<&Decision> = replayed
        .pairs
        .iter()
        .map(|pair| pair.baseline.as_ref().unwrap())
        .collect();

    assert_eq!(batch.schema(), replay::replay_baselines_schema());
    assert_eq!(
        u64_values(&batch, "batch_duration_ns"),
        [Some(300 * MS), Some(300 * MS), Some(S), Some(S)]
    );
    assert_eq!(
        u64_values(&batch, "window_groups"),
        [Some(2), Some(5), Some(2), Some(5)]
    );
    assert_eq!(
        f64_values(&batch, "steady_state_throughput"),
        baselines
            .iter()
            .map(|decision| decision.steady_state_throughput)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        f64_values(&batch, "relative_half_width"),
        baselines
            .iter()
            .map(|decision| decision.relative_half_width)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "warmup_end_ns"),
        baselines
            .iter()
            .map(|decision| decision.warmup_end_ns)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "window_end_ns"),
        [run.first_partition_end_ns; 4]
    );
    let capped_at_ns = run.samples.get(100).map(|sample| sample.elapsed_ns);
    assert_eq!(u64_values(&batch, "capped_at_ns"), [capped_at_ns; 4]);
}

#[test]
fn the_replays_table_holds_each_combination_s_would_stop_and_stop_reason() {
    let (_, replayed) = replayed_run();
    let batch = replayed.replays_batch().unwrap();
    let combinations: Vec<_> = replayed
        .pairs
        .iter()
        .flat_map(|pair| &pair.combinations)
        .collect();
    let would_stop = || combinations.iter().map(|row| row.would_stop.as_ref());

    assert_eq!(batch.schema(), replay::replays_schema());
    assert_eq!(batch.num_rows(), 32);
    assert_eq!(
        f64_values(&batch, "precision"),
        combinations
            .iter()
            .map(|row| Some(row.settings.precision))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "consecutive_checks"),
        combinations
            .iter()
            .map(|row| Some(u64::from(row.settings.consecutive_checks.get())))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "min_duration_ns"),
        combinations
            .iter()
            .map(|row| Some(u64::try_from(row.settings.min_duration.as_nanos()).unwrap()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "would_stop_ns"),
        would_stop()
            .map(|check| check.map(|check| check.elapsed_ns))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        f64_values(&batch, "would_be_steady_state_throughput"),
        would_stop()
            .map(|check| check.and_then(|check| check.steady_state_throughput))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        f64_values(&batch, "would_be_relative_half_width"),
        would_stop()
            .map(|check| check.and_then(|check| check.relative_half_width))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        u64_values(&batch, "would_be_warmup_end_ns"),
        would_stop()
            .map(|check| check.and_then(|check| check.warmup_end_ns))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        string_values(&batch, "probe_stop_reason"),
        combinations
            .iter()
            .map(|row| row.probe_stop_reason.name())
            .collect::<Vec<_>>()
    );
}
