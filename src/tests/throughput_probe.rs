use crate::throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason};

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

/// More groups than the window has samples leave some group spanning no time, so the check is
/// never tight, and a group count far past the samples costs no more than one within them.
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
