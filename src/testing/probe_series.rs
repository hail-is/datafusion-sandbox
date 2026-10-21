//! Synthetic series of progress samples, and the decisions a probe would make over a series.

use crate::throughput_probe::{self, Decision, ProbeSettings, ProgressSample};

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
pub(crate) fn noisy_series(seed: u64, ramp: u64, noise: f64) -> Vec<ProgressSample> {
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
pub(crate) fn recorded_would_stop(
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
pub(crate) fn first_decision(
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
