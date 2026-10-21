//! Synthetic series of progress samples, and the decisions a probe makes over a series.

use crate::{
    testing::probe_series::{first_decision, noisy_series, recorded_would_stop},
    throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason},
};

use std::{num::NonZeroU32, time::Duration};

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

#[test]
fn a_series_depends_only_on_its_arguments() {
    assert_eq!(noisy_series(7, 3, 0.1), noisy_series(7, 3, 0.1));
    assert_ne!(noisy_series(7, 3, 0.1), noisy_series(8, 3, 0.1));
}

#[test]
fn a_series_starts_empty_and_polls_every_100_to_150_ms_for_25_s() {
    for (seed, ramp, noise) in [(1, 0, 0.0), (2, 4, 0.05), (3, 6, 0.3)] {
        let samples = noisy_series(seed, ramp, noise);
        let context = format!("seed {seed}, ramp {ramp}, noise {noise}");

        assert_eq!(
            samples.first(),
            Some(&ProgressSample {
                elapsed_ns: 0,
                rows: 0
            }),
            "{context}"
        );
        for pair in samples.windows(2) {
            let [earlier, later] = pair else {
                panic!("windows of two")
            };
            let gap_ns = later.elapsed_ns.checked_sub(earlier.elapsed_ns).unwrap();
            assert!(
                (100 * MS..150 * MS).contains(&gap_ns),
                "{context}: {pair:?}"
            );
            assert!(later.rows >= earlier.rows, "{context}: {pair:?}");
        }
        let [.., before_last, last] = samples.as_slice() else {
            panic!("{context}: too few samples")
        };
        assert!(before_last.elapsed_ns < 25 * SECOND, "{context}");
        assert!(last.elapsed_ns >= 25 * SECOND, "{context}");
    }
}

/// The rows per second between the first sample at or past `from_ns` and the last sample.
fn rate_after(samples: &[ProgressSample], from_ns: u64) -> f64 {
    let first = samples
        .iter()
        .find(|sample| sample.elapsed_ns >= from_ns)
        .unwrap();
    rate_between(first, samples.last().unwrap())
}

fn rate_between(first: &ProgressSample, last: &ProgressSample) -> f64 {
    let rows = last.rows.checked_sub(first.rows).unwrap();
    let seconds =
        Duration::from_nanos(last.elapsed_ns.checked_sub(first.elapsed_ns).unwrap()).as_secs_f64();
    f64::from(u32::try_from(rows).unwrap()) / seconds
}

#[test]
fn after_its_ramp_a_series_runs_at_1000_rows_per_second_within_its_noise() {
    for (seed, ramp, noise) in [(1, 0, 0.0), (2, 4, 0.05), (3, 6, 0.3)] {
        let rate = rate_after(&noisy_series(seed, ramp, noise), ramp * SECOND);

        // One row of slack for the rows each end truncates.
        let slack = 1.0;
        assert!(
            rate >= 1_000.0f64.mul_add(-noise, 1_000.0) - slack
                && rate <= 1_000.0f64.mul_add(noise, 1_000.0) + slack,
            "seed {seed}, ramp {ramp}, noise {noise}: {rate} rows/s"
        );
    }
}

#[test]
fn during_its_first_ramp_second_a_series_runs_at_its_share_of_the_full_rate() {
    // A ramp of four seconds runs its first second at a fifth of the full rate.
    let samples = noisy_series(1, 4, 0.0);
    let first_second_end = samples
        .iter()
        .find(|sample| sample.elapsed_ns >= SECOND)
        .unwrap();

    let rate = rate_between(samples.first().unwrap(), first_second_end);

    assert!((rate - 200.0).abs() <= 2.0, "{rate} rows/s");
}

/// The decision `judge` first makes over the samples up to and including some sample of
/// `samples`, and how many samples it took. Each shorter prefix decides nothing.
fn first_by_prefix(
    samples: &[ProgressSample],
    judge: impl Fn(&[ProgressSample]) -> Option<Decision>,
) -> Option<(usize, Decision)> {
    (1..=samples.len()).find_map(|taken| Some((taken, judge(&samples[..taken])?)))
}

/// Settings under which [`noisy_series`] settles quickly.
fn eager_settings() -> ProbeSettings {
    ProbeSettings {
        batch_duration: Duration::from_millis(300),
        precision: 0.3,
        consecutive_checks: NonZeroU32::MIN,
        window_groups: 5,
        min_duration: Duration::ZERO,
        ..ProbeSettings::default()
    }
}

/// Settings under which no check is tight and nothing is capped within a series, so only a
/// partition end stops a probe.
fn never_steady_settings() -> ProbeSettings {
    ProbeSettings {
        precision: 0.0,
        max_duration: Duration::from_secs(3_600),
        ..ProbeSettings::default()
    }
}

#[test]
fn recorded_would_stop_is_the_first_steady_decision_over_the_samples_so_far() {
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let (taken, expected) = first_by_prefix(&samples, |seen| {
        throughput_probe::would_stop(&settings, seen, None)
    })
    .expect("eager settings settle within the series");
    assert!(taken < samples.len(), "the series goes on after the stop");

    let recorded = recorded_would_stop(&settings, &samples, None);

    assert_eq!(recorded, Some(expected));
}

#[test]
fn recorded_would_stop_takes_a_partition_end_only_once_a_sample_has_shown_it() {
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let unended = recorded_would_stop(&settings, &samples, None).unwrap();
    let stop_ns = unended.window_end_ns;

    // A partition end shown after the steady stop leaves it; one shown at or before it means no
    // steady decision was ever made.
    let after = recorded_would_stop(&settings, &samples, Some(stop_ns + 1));
    let at = recorded_would_stop(&settings, &samples, Some(stop_ns));

    assert_eq!(after, Some(unended));
    assert_eq!(at, None);
}

#[test]
fn first_decision_is_the_first_decision_over_the_samples_so_far() {
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let (_, expected) = first_by_prefix(&samples, |seen| {
        throughput_probe::decide(&settings, seen, None)
    })
    .unwrap();

    assert_eq!(first_decision(&settings, &samples, None), Some(expected));
}

#[test]
fn first_decision_caps_at_the_first_sample_at_or_past_the_maximum_duration() {
    let settings = ProbeSettings {
        max_duration: Duration::from_secs(10),
        ..never_steady_settings()
    };
    let samples = noisy_series(1, 0, 0.0);
    let capping = samples
        .iter()
        .find(|sample| sample.elapsed_ns >= 10 * SECOND)
        .unwrap();

    let decision = first_decision(&settings, &samples, None).unwrap();

    assert_eq!(decision.stop_reason, StopReason::Capped);
    assert_eq!(decision.window_end_ns, capping.elapsed_ns);
}

#[test]
fn first_decision_completes_once_a_sample_has_shown_the_partition_end() {
    let settings = never_steady_settings();
    let samples = noisy_series(1, 0, 0.0);
    assert_eq!(first_decision(&settings, &samples, None), None);

    // An end between two samples is shown by the later one, and one at a sample by that sample.
    for (end_ns, showing) in [
        (samples[40].elapsed_ns - 1, 40),
        (samples[40].elapsed_ns, 40),
    ] {
        let decision = first_decision(&settings, &samples, Some(end_ns));

        assert_eq!(
            decision,
            throughput_probe::decide(&settings, &samples[..=showing], Some(end_ns)),
            "end at {end_ns} ns"
        );
        assert_eq!(
            decision.map(|decision| decision.stop_reason),
            Some(StopReason::Completed)
        );
    }
}
