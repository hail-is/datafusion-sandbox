use crate::{
    tests::support::{noisy_series, recorded_would_stop},
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason, TightnessCheck},
};

use std::{num::NonZeroU32, time::Duration};

const MS: u64 = 1_000_000;

/// Unevenly spaced samples that start after execution did, with rows already received.
fn samples() -> Vec<ProgressSample> {
    [(10 * MS, 20), (110 * MS, 120), (410 * MS, 720)]
        .into_iter()
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows })
        .collect()
}

fn settings(max_duration_ms: u64) -> ProbeSettings {
    ProbeSettings {
        max_duration: Duration::from_millis(max_duration_ms),
        ..ProbeSettings::default()
    }
}

/// Samples every 100 ms from the start of execution, where each second's batch has the given rate
/// in rows per second.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "made-up series are far too short to overflow"
)]
fn series(rates: &[u64]) -> Vec<ProgressSample> {
    let mut rows = 0;
    let mut samples = vec![ProgressSample {
        elapsed_ns: 0,
        rows: 0,
    }];
    for (second, rate) in (0..).zip(rates) {
        for tenth in 1..=10 {
            rows += rate / 10;
            samples.push(ProgressSample {
                elapsed_ns: second * 1_000 * MS + tenth * 100 * MS,
                rows,
            });
        }
    }
    samples
}

/// Settings that stop at the first tight check, whatever the elapsed time.
fn eager() -> ProbeSettings {
    ProbeSettings {
        consecutive_checks: NonZeroU32::MIN,
        min_duration: Duration::ZERO,
        ..ProbeSettings::default()
    }
}

/// A ramp of 4 batches, then a flat stretch of 16: the warmup ends with the ramp, and the flat
/// stretch after it is tight at once.
#[test]
fn a_ramp_then_a_flat_stretch_puts_the_end_of_warmup_at_the_end_of_the_ramp() {
    let mut rates = vec![100, 200, 300, 400];
    rates.extend([1_000; 16]);

    assert_eq!(
        throughput_probe::decide(&eager(), &series(&rates), None),
        Some(Decision {
            stop_reason: StopReason::Steady,
            steady_state_throughput: Some(1_000.0),
            warmup_end_ns: Some(4_000 * MS),
            window_end_ns: 20_000 * MS,
            window_rows: 16_000,
            relative_half_width: Some(0.0),
        })
    );
}

/// A ramp of 12 batches then a flat stretch of 8 puts the MSER minimum in the second half, which
/// says the run is still too short to judge, however tight the flat stretch is.
#[test]
fn keeps_running_while_the_mser_minimum_lies_past_half_the_batches() {
    let mut rates: Vec<u64> = (1..=12).map(|batch| batch * 100).collect();
    rates.extend([1_300; 8]);

    assert_eq!(
        throughput_probe::decide(&eager(), &series(&rates), None),
        None
    );
}

/// Capped without an end of warmup, the estimate covers every sample.
#[test]
fn a_cap_without_an_end_of_warmup_estimates_over_every_sample() {
    let rates: Vec<u64> = (1..=20).map(|batch| batch * 100).collect();
    let settings = ProbeSettings {
        max_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &series(&rates), None),
        Some(Decision {
            stop_reason: StopReason::Capped,
            steady_state_throughput: Some(1_050.0),
            warmup_end_ns: None,
            window_end_ns: 20_000 * MS,
            window_rows: 21_000,
            relative_half_width: Some(0.412_550_781_555_502_5),
        })
    );
}

/// A ramp of 4 batches then a flat stretch, `batches` long in all.
fn ramp_then_flat(batches: usize) -> Vec<ProgressSample> {
    let mut rates = vec![100, 200, 300, 400];
    rates.resize(batches, 1_000);
    series(&rates)
}

/// The steady decision over [`ramp_then_flat`] of 20 batches.
fn settled_at_20_s() -> Decision {
    Decision {
        stop_reason: StopReason::Steady,
        steady_state_throughput: Some(1_000.0),
        warmup_end_ns: Some(4_000 * MS),
        window_end_ns: 20_000 * MS,
        window_rows: 16_000,
        relative_half_width: Some(0.0),
    }
}

/// A rate that alternates between 500 and 1,500 rows/s every second is noisy over 10 groups of
/// one second, and tight over 10 groups of two, each of which holds one second of each rate.
#[test]
fn keeps_running_through_a_noisy_stretch_until_the_interval_tightens() {
    let rates: Vec<u64> = [500, 1_500].into_iter().cycle().take(20).collect();
    let samples = series(&rates);

    assert_eq!(
        throughput_probe::decide(&eager(), samples.get(..=100).unwrap(), None),
        None
    );
    assert_eq!(
        throughput_probe::decide(&eager(), &samples, None),
        Some(Decision {
            stop_reason: StopReason::Steady,
            steady_state_throughput: Some(1_000.0),
            warmup_end_ns: Some(0),
            window_end_ns: 20_000 * MS,
            window_rows: 20_000,
            relative_half_width: Some(0.0),
        })
    );
}

/// After a ramp of 12 batches, the MSER minimum first lies in the first half at 24 batches, so
/// the check there is the first tight one, and three consecutive tight checks take until 26.
#[test]
fn a_single_tight_check_does_not_stop_a_probe_that_needs_several() {
    let mut rates: Vec<u64> = (1..=12).map(|batch| batch * 100).collect();
    rates.extend([1_300; 14]);
    let samples = series(&rates);
    let after = |batches: usize| samples.get(..=batches * 10).unwrap();
    let settings = ProbeSettings {
        consecutive_checks: NonZeroU32::new(3).unwrap(),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&eager(), after(24), None).map(|decision| decision.stop_reason),
        Some(StopReason::Steady)
    );
    assert_eq!(throughput_probe::decide(&settings, after(24), None), None);
    assert_eq!(throughput_probe::decide(&settings, after(25), None), None);
    assert_eq!(
        throughput_probe::decide(&settings, after(26), None),
        Some(Decision {
            stop_reason: StopReason::Steady,
            steady_state_throughput: Some(1_300.0),
            warmup_end_ns: Some(12_000 * MS),
            window_end_ns: 26_000 * MS,
            window_rows: 18_200,
            relative_half_width: Some(0.0),
        })
    );
}

/// Every check of a flat rate is tight, but the probe stops no sooner than its minimum duration.
#[test]
fn holds_the_minimum_duration() {
    let samples = series(&[1_000; 20]);
    let settings = ProbeSettings {
        min_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, samples.get(..=190).unwrap(), None),
        None
    );
    assert_eq!(
        throughput_probe::decide(&settings, &samples, None).map(|decision| decision.stop_reason),
        Some(StopReason::Steady)
    );
}

/// The rule checks at batch ends, so a minimum duration passed mid-batch waits for the next one.
#[test]
fn stops_steady_only_at_a_batch_end() {
    let samples = series(&[1_000; 20]);
    let settings = ProbeSettings {
        min_duration: Duration::from_millis(19_500),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, samples.get(..=195).unwrap(), None),
        None
    );
    assert_eq!(
        throughput_probe::decide(&settings, &samples, None).map(|decision| decision.window_end_ns),
        Some(20_000 * MS)
    );
}

/// Capped before it may stop steady, the probe still estimates over the window after the end of
/// warmup.
#[test]
fn caps_at_the_maximum_duration_with_the_estimate_after_warmup() {
    let settings = ProbeSettings {
        min_duration: Duration::from_secs(30),
        max_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &ramp_then_flat(20), None),
        Some(Decision {
            stop_reason: StopReason::Capped,
            ..settled_at_20_s()
        })
    );
}

/// Polled unevenly, a tenth of a second after each batch end and then at the next, a flat stretch
/// has per-poll rates of 5,000 and 556 rows/s, whose mean is far from its rate; the estimate is
/// still the rows over the time between the window's ends.
#[test]
fn unevenly_spaced_samples_give_the_exact_rows_over_time_estimate() {
    let mut samples = series(&[100, 200, 300, 400]);
    samples.extend((4..20).flat_map(|second: u64| {
        let rows = (second - 2) * 1_000;
        [
            ProgressSample {
                elapsed_ns: second * 1_000 * MS + 100 * MS,
                rows: rows - 500,
            },
            ProgressSample {
                elapsed_ns: (second + 1) * 1_000 * MS,
                rows,
            },
        ]
    }));
    let settings = ProbeSettings {
        min_duration: Duration::from_secs(30),
        max_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &samples, None),
        Some(Decision {
            stop_reason: StopReason::Capped,
            relative_half_width: Some(0.183_743_086_094_726_3),
            ..settled_at_20_s()
        })
    );
}

/// A sample at the maximum duration that also ends the last of the consecutive tight checks stops
/// the probe steady: both endings take the same estimate, and only steady says it settled.
#[test]
fn a_steady_stop_at_the_maximum_duration_is_steady() {
    let settings = ProbeSettings {
        max_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &ramp_then_flat(20), None),
        Some(settled_at_20_s())
    );
}

/// More groups than the window has samples leave some group spanning no time, so the window has
/// no estimate interval and the check is never tight, and a group count far past the samples costs
/// no more than one within them.
#[test]
fn more_window_groups_than_samples_are_never_tight() {
    let settings = ProbeSettings {
        window_groups: u32::MAX,
        max_duration: Duration::from_secs(20),
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &ramp_then_flat(20), None),
        Some(Decision {
            stop_reason: StopReason::Capped,
            relative_half_width: None,
            ..settled_at_20_s()
        })
    );
}

/// The first partition end closes the window, so the rows after it, at another rate, leave the
/// estimate and the end of warmup alone.
#[test]
fn completes_at_the_first_partition_end_with_the_estimate_after_warmup() {
    let mut samples = ramp_then_flat(20);
    samples.extend((1..=5).map(|tenth| ProgressSample {
        elapsed_ns: 20_000 * MS + tenth * 100 * MS,
        rows: 16_000 + 1_000 + tenth * 10,
    }));

    assert_eq!(
        throughput_probe::decide(&ProbeSettings::default(), &samples, Some(20_000 * MS)),
        Some(Decision {
            stop_reason: StopReason::Completed,
            ..settled_at_20_s()
        })
    );
}

/// Deciding after each sample, as a probe does, the default rule first stops at the first batch
/// end past its minimum duration. That the recorded tables replay to the decision is a combiner
/// run test.
#[test]
fn deciding_after_each_sample_stops_at_the_first_batch_end_past_the_minimum_duration() {
    let samples = ramp_then_flat(30);

    assert_eq!(
        (1..=samples.len()).find_map(|taken| {
            throughput_probe::decide(&ProbeSettings::default(), samples.get(..taken)?, None)
        }),
        Some(settled_at_20_s())
    );
}

#[test]
fn keeps_running_before_the_cap_and_without_a_partition_end() {
    assert_eq!(
        throughput_probe::decide(&settings(500), &samples(), None),
        None
    );
}

/// Capped at the maximum duration, the estimate is the rows between the first and last samples
/// over the time between them, not the rows over the elapsed time.
#[test]
fn caps_at_the_maximum_duration_with_rows_over_time_between_the_window_ends() {
    assert_eq!(
        throughput_probe::decide(&settings(400), &samples(), None),
        Some(Decision {
            stop_reason: StopReason::Capped,
            steady_state_throughput: Some(1750.0),
            warmup_end_ns: None,
            window_end_ns: 410 * MS,
            window_rows: 700,
            relative_half_width: None,
        })
    );
}

/// The first partition end closes the window at the sample that showed it, whatever came later
/// and even past the cap.
#[test]
fn completes_at_the_first_partition_end_which_closes_the_window() {
    assert_eq!(
        throughput_probe::decide(&settings(400), &samples(), Some(110 * MS)),
        Some(Decision {
            stop_reason: StopReason::Completed,
            steady_state_throughput: Some(1000.0),
            warmup_end_ns: None,
            window_end_ns: 110 * MS,
            window_rows: 100,
            relative_half_width: None,
        })
    );
}

/// A shadow evaluation ignores the maximum duration: where the probe caps, it keeps running, and
/// it stops steady where a probe that had not been capped would.
#[test]
fn a_shadow_evaluation_ignores_the_maximum_duration() {
    let settings = ProbeSettings {
        max_duration: Duration::from_secs(10),
        ..ProbeSettings::default()
    };
    let samples = ramp_then_flat(20);
    let (capped, settled) = (samples.get(..=110).unwrap(), samples.as_slice());

    assert_eq!(
        throughput_probe::decide(&settings, capped, None).map(|decision| decision.stop_reason),
        Some(StopReason::Capped)
    );
    assert_eq!(throughput_probe::would_stop(&settings, capped, None), None);
    assert_eq!(
        throughput_probe::would_stop(&settings, settled, None),
        Some(settled_at_20_s())
    );
}

/// In a shadow evaluation, the first partition end closes the window but does not stop: where
/// the probe completes, it keeps running, and since a probe would have completed there, no later
/// sample stops it steady. At the end of the run, the estimate is still over the closed window.
#[test]
fn a_first_partition_end_closes_a_shadow_evaluation_s_window_without_stopping_it() {
    let settings = ProbeSettings::default();
    let samples = ramp_then_flat(30);
    let first_partition_end_ns = Some(19_000 * MS);
    let settled = samples.get(..=200).unwrap();

    assert_eq!(
        throughput_probe::would_stop(&settings, settled, None),
        Some(settled_at_20_s())
    );
    assert_eq!(
        throughput_probe::would_stop(&settings, settled, first_partition_end_ns),
        None
    );
    assert_eq!(
        throughput_probe::decide(&settings, &samples, first_partition_end_ns),
        Some(Decision {
            stop_reason: StopReason::Completed,
            window_end_ns: 19_000 * MS,
            window_rows: 15_000,
            ..settled_at_20_s()
        })
    );
}

/// A window of one sample spans no time, so it has no estimate.
#[test]
fn a_window_of_one_sample_has_no_estimate() {
    assert_eq!(
        throughput_probe::decide(&settings(400), &samples()[..1], Some(10 * MS)),
        Some(Decision {
            stop_reason: StopReason::Completed,
            steady_state_throughput: None,
            warmup_end_ns: None,
            window_end_ns: 10 * MS,
            window_rows: 0,
            relative_half_width: None,
        })
    );
}

/// The defaults the README's probe flag table lists.
#[test]
fn defaults_every_setting_to_its_documented_value() {
    assert_eq!(
        ProbeSettings::default(),
        ProbeSettings {
            poll_period: Duration::from_millis(100),
            batch_duration: Duration::from_secs(1),
            precision: 0.02,
            consecutive_checks: NonZeroU32::new(3).unwrap(),
            window_groups: 10,
            min_duration: Duration::from_secs(20),
            max_duration: Duration::from_secs(300),
        }
    );
}

/// A ramp of 4 batches, then 16 that alternate between 950 and 1,050 rows/s, whose estimate
/// interval has some width.
fn ramp_then_noisy() -> Vec<ProgressSample> {
    let mut rates = vec![100, 200, 300, 400];
    rates.extend([950, 1_050].into_iter().cycle().take(16));
    series(&rates)
}

/// The recorded relative half-width is the one the tightness check compared with the precision:
/// a precision equal to it is not tight, and the next precision up is.
#[test]
fn a_decision_records_the_relative_half_width_its_check_compared() {
    let samples = ramp_then_noisy();
    let with_precision = |precision| ProbeSettings {
        precision,
        ..eager()
    };
    let recorded = throughput_probe::decide(&with_precision(1.0), &samples, None)
        .and_then(|decision| decision.relative_half_width)
        .unwrap();

    assert!(recorded > 0.0);
    assert_eq!(
        throughput_probe::decide(&with_precision(recorded), &samples, None),
        None
    );
    assert_eq!(
        throughput_probe::decide(&with_precision(recorded.next_up()), &samples, None)
            .map(|decision| (decision.stop_reason, decision.relative_half_width)),
        Some((StopReason::Steady, Some(recorded)))
    );
}

/// A shadow evaluation's decision records the same relative half-width as a probe's over the
/// same samples.
#[test]
fn a_shadow_evaluation_records_the_relative_half_width_a_probe_would() {
    let samples = ramp_then_noisy();
    let settings = ProbeSettings {
        precision: 1.0,
        ..eager()
    };

    assert_eq!(
        throughput_probe::would_stop(&settings, &samples, None),
        throughput_probe::decide(&settings, &samples, None)
    );
}

/// With two groups, the halfway boundary snaps to the first of two equally near samples, so the
/// first group spans no time and the window has no estimate interval.
#[test]
fn a_group_spanning_no_time_leaves_no_relative_half_width() {
    let samples: Vec<ProgressSample> = [(0, 0), (10_000 * MS, 1_000), (10_000 * MS + 1, 1_000)]
        .into_iter()
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows })
        .collect();
    let settings = ProbeSettings {
        window_groups: 2,
        max_duration: Duration::ZERO,
        ..eager()
    };

    assert_eq!(
        throughput_probe::decide(&settings, &samples, None)
            .map(|decision| (decision.stop_reason, decision.relative_half_width)),
        Some((StopReason::Capped, None))
    );
}

/// A tightness check at `seconds` into a probe polled every 100 ms, with an end of warmup at the
/// first sample if `warmed_up`.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "made-up checks are far too early to overflow"
)]
fn check(seconds: u64, warmed_up: bool, relative_half_width: Option<f64>) -> TightnessCheck {
    TightnessCheck {
        elapsed_ns: seconds * 1_000 * MS,
        sample_index: usize::try_from(seconds * 10).unwrap(),
        warmup_end_ns: warmed_up.then_some(0),
        steady_state_throughput: Some(1_000.0),
        relative_half_width,
    }
}

/// Settings that judge the stop with `precision`, `consecutive` checks and a minimum duration of
/// `min_seconds`.
fn judging(precision: f64, consecutive: u32, min_seconds: u64) -> ProbeSettings {
    ProbeSettings {
        precision,
        consecutive_checks: NonZeroU32::new(consecutive).unwrap(),
        min_duration: Duration::from_secs(min_seconds),
        ..ProbeSettings::default()
    }
}

/// Tightness checks, one a second for 12 seconds, at each edge of the stop: no end of warmup at
/// 1 s, no relative half-width at 2 s, one of 0.02 at 3 s, three of 0.01 from 4 s, one of 0.03 at
/// 7 s, and five of 0.005 from 8 s.
fn edge_checks() -> Vec<TightnessCheck> {
    let mut checks = vec![
        check(1, false, Some(0.001)),
        check(2, true, None),
        check(3, true, Some(0.02)),
    ];
    checks.extend((4..=6).map(|seconds| check(seconds, true, Some(0.01))));
    checks.push(check(7, true, Some(0.03)));
    checks.extend((8..=12).map(|seconds| check(seconds, true, Some(0.005))));
    checks
}

/// The elapsed nanoseconds of the check that stops a probe with `settings` over `checks`.
fn stop_ns(
    settings: &ProbeSettings,
    checks: &[TightnessCheck],
    first_partition_end_ns: Option<u64>,
) -> Option<u64> {
    throughput_probe::stop(settings, checks, first_partition_end_ns).map(|check| check.elapsed_ns)
}

#[test]
fn a_check_without_an_end_of_warmup_fails_at_any_precision() {
    let unwarmed = check(1, false, Some(0.0));

    assert!(!unwarmed.passes(1.0));
    assert_eq!(stop_ns(&judging(0.002, 1, 0), &edge_checks(), None), None);
}

#[test]
fn a_check_without_a_relative_half_width_fails_at_any_precision() {
    assert!(!check(2, true, None).passes(f64::MAX));
}

/// A check passes only when its relative half-width is strictly below the precision.
#[test]
fn a_relative_half_width_equal_to_the_precision_fails() {
    let checks = edge_checks();

    assert_eq!(
        stop_ns(&judging(0.02, 1, 0), &checks, None),
        Some(4_000 * MS)
    );
    assert_eq!(
        stop_ns(&judging(0.05, 1, 0), &checks, None),
        Some(3_000 * MS)
    );
}

#[test]
fn stops_at_the_first_check_that_ends_a_streak_of_consecutive_passes() {
    let checks = edge_checks();

    assert_eq!(
        stop_ns(&judging(0.02, 3, 0), &checks, None),
        Some(6_000 * MS)
    );
    assert_eq!(
        stop_ns(&judging(0.05, 5, 0), &checks, None),
        Some(7_000 * MS)
    );
    assert_eq!(stop_ns(&judging(0.02, 10, 0), &checks, None), None);
}

/// A failed check ends the streak: three passes from 4 s and a failure at 7 s leave four
/// consecutive passes to the checks from 8 s, though four checks have passed by then.
#[test]
fn a_failed_check_starts_the_streak_again() {
    assert_eq!(
        stop_ns(&judging(0.02, 4, 0), &edge_checks(), None),
        Some(11_000 * MS)
    );
}

/// Passes before the minimum duration count toward the streak: the checks at 8 and 9 s pass
/// before a minimum duration of 10 s, so the check at 10 s ends a streak of three.
#[test]
fn passes_before_the_minimum_duration_count_toward_the_streak() {
    assert_eq!(
        stop_ns(&judging(0.02, 3, 10), &edge_checks(), None),
        Some(10_000 * MS)
    );
}

/// The check at the sample that showed the first partition end would end a streak of three, but
/// a probe would have completed there.
#[test]
fn no_check_at_or_after_the_first_partition_end_stops_the_probe() {
    let checks = edge_checks();
    let settings = judging(0.02, 3, 0);

    assert_eq!(
        stop_ns(&settings, &checks, Some(6_000 * MS + 1)),
        Some(6_000 * MS)
    );
    assert_eq!(stop_ns(&settings, &checks, Some(6_000 * MS)), None);
}

#[test]
fn a_stop_no_later_than_the_cap_is_steady() {
    assert_eq!(
        throughput_probe::probe_stop_reason(
            Some(5 * 1_000 * MS),
            Some(6 * 1_000 * MS),
            Some(7 * 1_000 * MS)
        ),
        StopReason::Steady
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(
            Some(6 * 1_000 * MS),
            Some(6 * 1_000 * MS),
            Some(7 * 1_000 * MS)
        ),
        StopReason::Steady
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(Some(6 * 1_000 * MS), None, Some(7 * 1_000 * MS)),
        StopReason::Steady
    );
}

#[test]
fn a_cap_before_the_stop_and_the_first_partition_end_is_capped() {
    assert_eq!(
        throughput_probe::probe_stop_reason(
            Some(7 * 1_000 * MS),
            Some(6 * 1_000 * MS),
            Some(8 * 1_000 * MS)
        ),
        StopReason::Capped
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(None, Some(6 * 1_000 * MS), Some(8 * 1_000 * MS)),
        StopReason::Capped
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(None, Some(6 * 1_000 * MS), None),
        StopReason::Capped
    );
}

/// The first partition end and the cap at the same sample complete the probe, as deciding there
/// does.
#[test]
fn a_first_partition_end_before_or_at_the_cap_completes() {
    assert_eq!(
        throughput_probe::probe_stop_reason(None, Some(6 * 1_000 * MS), Some(6 * 1_000 * MS)),
        StopReason::Completed
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(None, Some(6 * 1_000 * MS), Some(5 * 1_000 * MS)),
        StopReason::Completed
    );
    assert_eq!(
        throughput_probe::probe_stop_reason(None, None, Some(5 * 1_000 * MS)),
        StopReason::Completed
    );
}

/// The series and first partition ends the equivalence tests judge. A first partition end is a
/// sample's time, the sample that showed it.
fn judged_series() -> Vec<(Vec<ProgressSample>, Option<u64>)> {
    let ending_at = |samples: Vec<ProgressSample>, index: usize| {
        let end_ns = samples.get(index).map(|sample| sample.elapsed_ns);
        (samples, end_ns)
    };
    vec![
        (noisy_series(1, 0, 0.0), None),
        (noisy_series(2, 4, 0.05), None),
        (noisy_series(3, 6, 0.3), None),
        ending_at(noisy_series(4, 3, 0.1), 110),
        ending_at(noisy_series(5, 8, 0.02), 160),
        ending_at(noisy_series(6, 2, 0.05), 90),
    ]
}

/// Settings over every batch duration, window groups, precision, consecutive checks and minimum
/// duration the equivalence tests judge.
fn judged_settings() -> Vec<ProbeSettings> {
    let mut settings = Vec::new();
    for batch_ms in [300, 1_000, 2_500] {
        for window_groups in [2, 5, 10] {
            for precision in [0.02, 0.2] {
                for consecutive in [1, 3] {
                    for min_seconds in [0, 8] {
                        settings.push(ProbeSettings {
                            batch_duration: Duration::from_millis(batch_ms),
                            window_groups,
                            ..judging(precision, consecutive, min_seconds)
                        });
                    }
                }
            }
        }
    }
    settings
}

/// The estimate a decision takes, as a tightness check carries it.
fn estimate(decision: &Decision) -> (u64, Option<f64>, Option<u64>, Option<f64>) {
    (
        decision.window_end_ns,
        decision.steady_state_throughput,
        decision.warmup_end_ns,
        decision.relative_half_width,
    )
}

/// The estimate a tightness check carries.
fn check_estimate(check: &TightnessCheck) -> (u64, Option<f64>, Option<u64>, Option<f64>) {
    (
        check.elapsed_ns,
        check.steady_state_throughput,
        check.warmup_end_ns,
        check.relative_half_width,
    )
}

/// The stop over a series' tightness checks is where a shadow probe judging after every sample
/// first records a would-stop, with the same estimate.
#[test]
fn the_stop_over_tightness_checks_is_the_first_would_stop_after_each_sample() {
    let mut stops = 0;
    for (samples, first_partition_end_ns) in judged_series() {
        for settings in judged_settings() {
            let checks = throughput_probe::tightness_checks(&settings, &samples);
            let replayed = recorded_would_stop(&settings, &samples, first_partition_end_ns);
            let stopped = throughput_probe::stop(&settings, &checks, first_partition_end_ns);

            stops += usize::from(stopped.is_some());
            assert_eq!(
                stopped.map(check_estimate),
                replayed.as_ref().map(estimate),
                "{settings:?}"
            );
        }
    }
    // The series stop under some settings and not under others.
    assert!(stops > 20, "{stops}");
    assert!(
        stops < judged_series().len() * judged_settings().len() - 20,
        "{stops}"
    );
}

/// Each tightness check carries the estimate the rule takes over the samples up to it, and
/// there is one at every probe batch end.
#[test]
fn a_tightness_check_carries_the_estimate_decided_at_its_batch_end() {
    for (samples, _) in judged_series() {
        for settings in judged_settings() {
            // Capped at once, the rule decides after every sample, steady or not.
            let settings = ProbeSettings {
                max_duration: Duration::ZERO,
                ..settings
            };
            let checks = throughput_probe::tightness_checks(&settings, &samples);
            let batch_ns = u64::try_from(settings.batch_duration.as_nanos()).unwrap();

            assert!(
                checks
                    .windows(2)
                    .all(|pair| pair[1].elapsed_ns - pair[0].elapsed_ns >= batch_ns),
                "{settings:?}"
            );
            for check in &checks {
                let decided = throughput_probe::decide(
                    &settings,
                    samples.get(..=check.sample_index).unwrap(),
                    None,
                )
                .unwrap();

                assert_eq!(check_estimate(check), estimate(&decided), "{settings:?}");
            }
        }
    }
}

/// The shared fixture of stops over tightness checks, which the probe viewer's copy of the stop
/// is tested against.
const STOP_FIXTURE: &str =
    include_str!("../../python/tests/fixtures/stop_over_tightness_checks.json");

/// Runs over [`edge_checks`], with their first partition end and the time of their first sample
/// at or past the maximum duration, at every edge of the stop reason under `judging(0.02, 3, 0)`,
/// which stops at 6 s: a cap after the stop, at it and before it, a first partition end at the
/// cap, and a streak that crosses the first partition end.
const FIXTURE_RUNS: [(&str, Option<u64>, Option<u64>); 6] = [
    ("uncapped", Some(12 * 1_000 * MS), None),
    (
        "capped_after_the_stop",
        Some(12 * 1_000 * MS),
        Some(8 * 1_000 * MS),
    ),
    (
        "capped_at_the_stop",
        Some(12 * 1_000 * MS),
        Some(6 * 1_000 * MS),
    ),
    (
        "capped_before_the_stop",
        Some(12 * 1_000 * MS),
        Some(5 * 1_000 * MS),
    ),
    (
        "completed_at_the_cap",
        Some(5 * 1_000 * MS),
        Some(5 * 1_000 * MS),
    ),
    (
        "a_streak_crosses_the_first_partition_end",
        Some(6 * 1_000 * MS),
        None,
    ),
];

/// The settings the fixture finds stops under, each at an edge of the stop over [`edge_checks`].
fn fixture_settings() -> Vec<ProbeSettings> {
    vec![
        judging(0.02, 1, 0),
        judging(0.05, 1, 0),
        judging(0.01, 1, 0),
        judging(0.002, 1, 0),
        judging(0.02, 3, 0),
        judging(0.02, 4, 0),
        judging(0.05, 5, 0),
        judging(0.02, 3, 10),
        judging(0.02, 10, 0),
    ]
}

/// The stop over [`edge_checks`] for a fixture run, and how a probe with `settings` would end.
fn fixture_stop(
    settings: &ProbeSettings,
    (_, first_partition_end_ns, capped_at_ns): (&str, Option<u64>, Option<u64>),
) -> (Option<u64>, StopReason) {
    let stop_ns = stop_ns(settings, &edge_checks(), first_partition_end_ns);
    (
        stop_ns,
        throughput_probe::probe_stop_reason(stop_ns, capped_at_ns, first_partition_end_ns),
    )
}

/// `value` as JSON: a number, or `null`.
fn json<T: std::fmt::Debug>(value: Option<T>) -> String {
    value.map_or_else(|| "null".to_string(), |value| format!("{value:?}"))
}

/// The JSON array of `rows`, one to a line.
fn json_rows(rows: &[String]) -> String {
    let rows: Vec<String> = rows.iter().map(|row| format!("    {row}")).collect();
    format!("[\n{}\n  ]", rows.join(",\n"))
}

/// The shared fixture as this rule generates it: the runs, their tightness checks in the columns
/// of the checks table, and the stop and stop reason under each setting in those of the replays
/// table.
fn stop_fixture_json() -> String {
    let runs: Vec<String> = FIXTURE_RUNS
        .iter()
        .map(|&(run_id, first_partition_end_ns, capped_at_ns)| {
            format!(
                r#"{{"run_id": "{run_id}", "first_partition_end_ns": {}, "capped_at_ns": {}}}"#,
                json(first_partition_end_ns),
                json(capped_at_ns)
            )
        })
        .collect();
    let checks: Vec<String> = FIXTURE_RUNS
        .iter()
        .flat_map(|&(run_id, ..)| {
            edge_checks().into_iter().enumerate().map(move |(index, check)| {
                format!(
                    r#"{{"run_id": "{run_id}", "check_index": {index}, "sample_index": {}, "elapsed_ns": {}, "warmup_end_ns": {}, "steady_state_throughput": {}, "relative_half_width": {}}}"#,
                    check.sample_index,
                    check.elapsed_ns,
                    json(check.warmup_end_ns),
                    json(check.steady_state_throughput),
                    json(check.relative_half_width)
                )
            })
        })
        .collect();
    let stops: Vec<String> = fixture_settings()
        .iter()
        .flat_map(|settings| {
            FIXTURE_RUNS.iter().map(move |&run| {
                let (stop_ns, reason) = fixture_stop(settings, run);
                format!(
                    r#"{{"run_id": "{}", "precision": {:?}, "consecutive_checks": {}, "min_duration_ns": {}, "would_stop_ns": {}, "probe_stop_reason": "{}"}}"#,
                    run.0,
                    settings.precision,
                    settings.consecutive_checks,
                    settings.min_duration.as_nanos(),
                    json(stop_ns),
                    reason.name()
                )
            })
        })
        .collect();
    format!(
        "{{\n  \"about\": \"Stops over tightness checks, generated by the module test the_shared_stop_fixture_is_current in src/tests/throughput_probe.rs. Do not edit by hand.\",\n  \"runs\": {},\n  \"checks\": {},\n  \"stops\": {}\n}}\n",
        json_rows(&runs),
        json_rows(&checks),
        json_rows(&stops)
    )
}

/// The checked-in fixture is the one this rule generates. When it is stale, the failure prints
/// the regenerated fixture to paste over it.
#[test]
fn the_shared_stop_fixture_is_current() {
    let regenerated = stop_fixture_json();

    assert!(
        STOP_FIXTURE == regenerated,
        "python/tests/fixtures/stop_over_tightness_checks.json is stale; replace it with:\n{regenerated}"
    );
}

/// The fixture's runs reach every ending at the edges of the stop reason: a stop before and at the
/// cap is steady, after it capped, and a cap at the first partition end, or a streak that would
/// end there, completed.
#[test]
fn the_shared_stop_fixture_ends_at_every_edge_of_the_stop_reason() {
    let settings = judging(0.02, 3, 0);

    assert_eq!(
        FIXTURE_RUNS
            .iter()
            .map(|&run| fixture_stop(&settings, run))
            .collect::<Vec<_>>(),
        [
            (Some(6_000 * MS), StopReason::Steady),
            (Some(6_000 * MS), StopReason::Steady),
            (Some(6_000 * MS), StopReason::Steady),
            (Some(6_000 * MS), StopReason::Capped),
            (None, StopReason::Completed),
            (None, StopReason::Completed),
        ]
    );
}
