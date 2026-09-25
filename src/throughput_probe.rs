//! A throughput probe's settings, its progress samples, and the decision to stop it.
//!
//! A throughput probe runs a formulation's plan, takes a [`ProgressSample`] every poll period,
//! and stops once [`decide`] says so. Stopping and sampling belong to [`crate::sink::probe`];
//! this module only judges the samples, so the decision is a pure function of the settings, the
//! samples so far, and the first partition end, and a recorded probe replays exactly.
//!
//! The rule batches the samples, picks the end of warmup with MSER over the batch rates, and
//! stops, steady, once a confidence interval for the rate over the measurement window that
//! follows has been tight at several consecutive batch ends. See
//! [ADR 0017](../docs/adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).

use std::{num::NonZeroU32, time::Duration};

/// How a throughput probe samples and when it gives up.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbeSettings {
    /// The time between progress samples.
    pub poll_period: Duration,
    /// The least time a batch of progress samples spans. MSER judges warmup over the batches'
    /// rates, and the rule checks its interval at the end of each batch.
    pub batch_duration: Duration,
    /// The relative half-width of the interval below which a check is tight.
    pub precision: f64,
    /// How many consecutive checks must be tight for the probe to stop, steady.
    pub consecutive_checks: NonZeroU32,
    /// How many groups of equal duration the measurement window splits into for its interval.
    /// An interval needs at least two, so with fewer no check is tight.
    pub window_groups: u32,
    /// The execution time before which the probe does not stop steady.
    pub min_duration: Duration,
    /// The execution time at which the probe stops, capped, unless the same sample stops it
    /// steady.
    pub max_duration: Duration,
}

impl Default for ProbeSettings {
    fn default() -> Self {
        Self {
            poll_period: Duration::from_millis(100),
            batch_duration: Duration::from_secs(1),
            precision: 0.02,
            consecutive_checks: NonZeroU32::MIN.saturating_add(2),
            window_groups: 10,
            min_duration: Duration::from_secs(20),
            max_duration: Duration::from_secs(300),
        }
    }
}

/// One reading of how many rows the sink has received, and when.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressSample {
    /// Nanoseconds since execution started, taken when the rows were read.
    pub elapsed_ns: u64,
    /// The rows the operator feeding the sink had emitted, over all its partitions.
    pub rows: u64,
}

/// Why a throughput probe ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// The estimate settled: its interval was tight at enough consecutive checks.
    Steady,
    /// A partition of the plan finished, closing the measurement window.
    Completed,
    /// The probe reached its maximum duration.
    Capped,
}

impl StopReason {
    /// The stop reason as the run record and the CLI spell it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Completed => "completed",
            Self::Capped => "capped",
        }
    }
}

/// A decision to stop a throughput probe, and its estimate over the measurement window.
#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    pub stop_reason: StopReason,
    /// The rows the sink received per second between the window's end samples; `None` when
    /// they are one sample, which spans no time.
    pub steady_state_throughput: Option<f64>,
    /// The elapsed nanoseconds of the sample that ends warmup and starts the window; `None` when
    /// MSER found no end of warmup, and the window starts at the first sample.
    pub warmup_end_ns: Option<u64>,
    /// The elapsed nanoseconds of the window's last sample.
    pub window_end_ns: u64,
    /// The rows the sink received between the window's end samples.
    pub window_rows: u64,
}

/// Whether a probe with `settings` stops after `samples`, in the order taken, given the elapsed
/// nanoseconds of the first sample that showed a finished partition, if one has. `None` keeps
/// it running.
///
/// The probe completes at the first partition end, whose sample closes the window. Otherwise it
/// stops steady at a batch end past the minimum duration once the last `consecutive_checks` were
/// tight, and is capped once its latest sample is at or past the maximum duration. A sample that
/// does both stops it steady, since both take the same estimate and only steady says it settled.
/// Whatever the reason, the estimate is over the window after the current end of warmup, or over
/// every sample if MSER has found none.
#[must_use]
pub fn decide(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> Option<Decision> {
    let latest = samples.last()?;
    // The samples the estimate may cover: up to the one that showed the first partition end.
    let (stop_reason, until_window_end) = match first_partition_end_ns {
        Some(end_ns) => (
            StopReason::Completed,
            samples.get(..samples.partition_point(|sample| sample.elapsed_ns <= end_ns))?,
        ),
        None if steady(settings, samples) => (StopReason::Steady, samples),
        None if Duration::from_nanos(latest.elapsed_ns) >= settings.max_duration => {
            (StopReason::Capped, samples)
        }
        None => return None,
    };
    let window = Window::after_warmup(settings, until_window_end)?;
    let (first, window_end) = (window.samples.first()?, window.samples.last()?);
    Some(Decision {
        stop_reason,
        steady_state_throughput: rate_between(first, window_end),
        warmup_end_ns: window.warmup_end_ns,
        window_end_ns: window_end.elapsed_ns,
        window_rows: window_end.rows.saturating_sub(first.rows),
    })
}

/// Whether `samples` end at a batch end past the minimum duration, and the checks at the last
/// `consecutive_checks` batch ends were all tight.
fn steady(settings: &ProbeSettings, samples: &[ProgressSample]) -> bool {
    let ends = batch_ends(settings.batch_duration, samples);
    let Some(latest) = samples.len().checked_sub(1) else {
        return false;
    };
    let consecutive = usize::try_from(settings.consecutive_checks.get()).unwrap_or(usize::MAX);
    ends.last() == Some(&latest)
        && samples
            .last()
            .is_some_and(|sample| Duration::from_nanos(sample.elapsed_ns) >= settings.min_duration)
        && ends.len() >= consecutive
        && ends.iter().rev().take(consecutive).all(|&end| {
            samples
                .get(..=end)
                .and_then(|checked| Window::after_warmup(settings, checked))
                .is_some_and(|window| window.warmup_end_ns.is_some() && window.tight(settings))
        })
}

/// The indexes of the samples that end each batch. A batch runs from the end of the one before,
/// or the first sample, to the first later sample at least the batch duration after it, so every
/// batch spans some time.
fn batch_ends(batch: Duration, samples: &[ProgressSample]) -> Vec<usize> {
    let mut start_ns = samples.first().map(|sample| sample.elapsed_ns);
    let mut ends = Vec::new();
    for (index, sample) in samples.iter().enumerate().skip(1) {
        if start_ns.is_some_and(|start_ns| {
            let span_ns = sample.elapsed_ns.saturating_sub(start_ns);
            span_ns > 0 && Duration::from_nanos(span_ns) >= batch
        }) {
            ends.push(index);
            start_ns = Some(sample.elapsed_ns);
        }
    }
    ends
}

/// The samples a check or an estimate covers.
struct Window<'a> {
    /// The elapsed nanoseconds of the first of `samples`, if MSER put the end of warmup there.
    warmup_end_ns: Option<u64>,
    /// The samples from the end of warmup, or the first sample, to the last one.
    samples: &'a [ProgressSample],
}

impl<'a> Window<'a> {
    /// The window of `samples` after the end of warmup MSER finds over their batches, or all of
    /// them if it finds none. `None` when there are no samples.
    fn after_warmup(settings: &ProbeSettings, samples: &'a [ProgressSample]) -> Option<Self> {
        let ends = batch_ends(settings.batch_duration, samples);
        let boundaries = || {
            std::iter::once(0)
                .chain(ends.iter().copied())
                .filter_map(|index| samples.get(index))
        };
        // Every batch spans some time, so none drops out of the rates.
        let rates: Vec<f64> = boundaries()
            .zip(boundaries().skip(1))
            .filter_map(|(start, end)| rate_between(start, end))
            .collect();
        let Some(warmup) = warmup_batches(&rates) else {
            samples.first()?;
            return Some(Self {
                warmup_end_ns: None,
                samples,
            });
        };
        let start = match warmup.checked_sub(1) {
            None => 0,
            Some(last_cut) => *ends.get(last_cut)?,
        };
        Some(Self {
            warmup_end_ns: Some(samples.get(start)?.elapsed_ns),
            samples: samples.get(start..)?,
        })
    }

    /// Whether the relative half-width of the window's interval is below the precision target.
    ///
    /// The window splits into `window_groups` groups of equal duration, their boundaries snapped
    /// to the nearest sample, and the interval is a 95% t-interval over the groups' rates. A group
    /// that snaps to no time gives no interval, as some must when there are as many groups as
    /// samples, which is checked first so that no group count costs more than the samples.
    fn tight(&self, settings: &ProbeSettings) -> bool {
        let (Some(first), Some(last)) = (self.samples.first(), self.samples.last()) else {
            return false;
        };
        let groups = settings.window_groups;
        if usize::try_from(groups).map_or(true, |groups| groups >= self.samples.len()) {
            return false;
        }
        let span_ns = last.elapsed_ns.saturating_sub(first.elapsed_ns);
        let boundaries: Vec<&ProgressSample> = (0..=groups)
            .filter_map(|group| {
                let offset_ns = u128::from(span_ns)
                    .saturating_mul(u128::from(group))
                    .checked_div(u128::from(groups))?;
                let target_ns = first
                    .elapsed_ns
                    .saturating_add(u64::try_from(offset_ns).ok()?);
                self.nearest(target_ns)
            })
            .collect();
        let rates: Option<Vec<f64>> = boundaries
            .iter()
            .zip(boundaries.iter().skip(1))
            .map(|(start, end)| rate_between(start, end))
            .collect();
        rates.is_some_and(|rates| relative_half_width(&rates) < settings.precision)
    }

    /// The window's sample nearest `target_ns`, the earlier of two equally near.
    fn nearest(&self, target_ns: u64) -> Option<&'a ProgressSample> {
        let after = self
            .samples
            .partition_point(|sample| sample.elapsed_ns < target_ns);
        let before = after
            .checked_sub(1)
            .and_then(|index| self.samples.get(index));
        match (before, self.samples.get(after)) {
            (Some(before), Some(after))
                if after.elapsed_ns.saturating_sub(target_ns)
                    < target_ns.saturating_sub(before.elapsed_ns) =>
            {
                Some(after)
            }
            (Some(before), _) => Some(before),
            (None, after) => after,
        }
    }
}

/// The fewest batches whose cut minimizes MSER over `rates`, if that minimum lies in the first
/// half of the batches.
///
/// `MSER(d)` is `Σ_{i>d} (Yᵢ − Ȳ_d)² / (n − d)²`, the squared standard error of the rates left after
/// cutting the first `d`. A cut leaving fewer than [`MSER_MIN_TAIL`] batches is not considered:
/// the variance of the last few rates is too noisy to compare, and that of the last one alone is
/// always zero, which would put every minimum past the half. A minimum past `n / 2` means the run
/// is still too short to judge, and gives `None`.
fn warmup_batches(rates: &[f64]) -> Option<usize> {
    // Welford's running variance over the tail, grown from the last rate backwards, so every
    // cut's MSER comes in one pass, and a constant tail's is exactly zero.
    let mut best: Option<(usize, f64)> = None;
    let (mut count, mut mean, mut squares) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (cut, &rate) in rates.iter().enumerate().rev() {
        count += 1.0;
        let delta = rate - mean;
        mean += delta / count;
        squares = delta.mul_add(rate - mean, squares);
        let mser = squares / (count * count);
        // Ties go to the fewer cut batches, the later one seen.
        if rates.len().saturating_sub(cut) >= MSER_MIN_TAIL
            && best.is_none_or(|(_, least)| mser <= least)
        {
            best = Some((cut, mser));
        }
    }
    best.map(|(cut, _)| cut)
        .filter(|&cut| cut <= rates.len().checked_div(2).unwrap_or(0))
}

/// The fewest batches a cut may leave for MSER to judge it.
const MSER_MIN_TAIL: usize = 5;

/// The half-width of a 95% t-interval for the mean of `rates` over that mean; infinite when the
/// mean is not positive or there are fewer than two rates.
fn relative_half_width(rates: &[f64]) -> f64 {
    let Some(freedom) = rates.len().checked_sub(1).filter(|&freedom| freedom > 0) else {
        return f64::INFINITY;
    };
    let (count, freedom) = (count_f64(rates.len()), count_f64(freedom));
    let mean = rates.iter().sum::<f64>() / count;
    let variance = rates.iter().map(|rate| (rate - mean).powi(2)).sum::<f64>() / freedom;
    let half_width = t_quantile(freedom) * (variance / count).sqrt();
    if mean > 0.0 {
        half_width / mean
    } else {
        f64::INFINITY
    }
}

/// The 97.5th percentile of Student's t with `freedom` degrees of freedom, from a table up to 30
/// and the Cornish-Fisher expansion about the normal's beyond, which is within 1e-5 there.
fn t_quantile(freedom: f64) -> f64 {
    const TABLE: [f64; 30] = [
        12.7062, 4.3027, 3.1824, 2.7764, 2.5706, 2.4469, 2.3646, 2.3060, 2.2622, 2.2281, 2.2010,
        2.1788, 2.1604, 2.1448, 2.1314, 2.1199, 2.1098, 2.1009, 2.0930, 2.0860, 2.0796, 2.0739,
        2.0687, 2.0639, 2.0595, 2.0555, 2.0518, 2.0484, 2.0452, 2.0423,
    ];
    // The normal's 97.5th percentile z, and the expansion's terms in 1/ν, 1/ν² and 1/ν³:
    // (z³ + z)/4, (5z⁵ + 16z³ + 3z)/96 and (3z⁷ + 19z⁵ + 17z³ − 15z)/384.
    const Z: f64 = 1.959_963_985;
    const TERMS: [f64; 3] = [2.372_271_230, 2.822_498_616, 2.555_849_680];
    TABLE
        .iter()
        .zip(1..)
        .find(|&(_, row)| f64::from(row) >= freedom)
        .map_or_else(
            || {
                let [first, second, third] = TERMS;
                Z + (first + (second + third / freedom) / freedom) / freedom
            },
            |(&quantile, _)| quantile,
        )
}

/// `count` as a float, exact below 2^53.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "counts of rates and groups are far below 2^53"
)]
const fn count_f64(count: usize) -> f64 {
    count as f64
}

/// The rows received per second from `start` to `end`; `None` when they span no time.
fn rate_between(start: &ProgressSample, end: &ProgressSample) -> Option<f64> {
    let ns = end.elapsed_ns.saturating_sub(start.elapsed_ns);
    (ns > 0).then(|| rate(end.rows.saturating_sub(start.rows), ns))
}

/// `rows` over `ns` nanoseconds, per second.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "a rate is an estimate; counts past 2^53 lose precision it does not have"
)]
fn rate(rows: u64, ns: u64) -> f64 {
    rows as f64 / (ns as f64 / 1e9)
}
