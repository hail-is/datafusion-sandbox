use crate::throughput_probe::{self, Decision, ProbeSettings, ProgressSample, StopReason};

use std::time::Duration;

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
            window_end_ns: 10 * MS,
            window_rows: 0,
        })
    );
}

#[test]
fn defaults_to_a_100_ms_poll_period_and_a_300_s_cap() {
    let settings = ProbeSettings::default();

    assert_eq!(settings.poll_period, Duration::from_millis(100));
    assert_eq!(settings.max_duration, Duration::from_secs(300));
}
