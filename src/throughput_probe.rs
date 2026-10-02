//! A throughput probe's settings, its progress samples, and the decision to stop it.
//!
//! A throughput probe runs a formulation's plan, takes a [`ProgressSample`] every poll period,
//! and stops once [`decide`] says so. Stopping and sampling belong to [`crate::sink::probe`];
//! this module only judges the samples, so the decision is a pure function of the settings, the
//! samples so far, and the first partition end, and a recorded probe replays exactly.
//!
//! The rule batches the samples, picks the end of warmup with MSER over the batch rates, and
//! stops, steady, once the estimate interval for the rate over the measurement window that
//! follows has been tight at several consecutive batch ends. See
//! [ADR 0017](../docs/adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).
//!
//! The rule takes two steps: [`tightness_checks`] judges each probe batch end, and [`stop`] finds
//! the check that stops the probe. [`decide`] and [`would_stop`] take them after every sample, and
//! a [replay](crate::replay) once over a whole run under many settings.

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
    /// The half-width of the window's estimate interval over its mean, as a tightness check of
    /// the window compares it with the precision, whatever the stop reason; `None` when the
    /// window has no interval, or its mean is not positive.
    pub relative_half_width: Option<f64>,
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
        None if at_or_past_max_duration(settings, latest) => (StopReason::Capped, samples),
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
        relative_half_width: window.relative_half_width(settings),
    })
}

/// Whether a throughput probe acts on its stopping rule, as a caller asks for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeKind {
    /// Stops at the rule's first decision.
    Probe,
    /// A shadow probe: runs to completion, recording when the rule would have stopped it.
    Shadow,
}

/// A throughput probe's kind as it ended, with what a shadow probe recorded.
#[derive(Clone, Debug, PartialEq)]
pub enum ProbedKind {
    Probe,
    /// A shadow probe, with the first decision [`would_stop`] made; `None` if its rule never
    /// stopped steady.
    Shadow {
        would_stop: Option<Decision>,
    },
}

impl ProbedKind {
    /// The name of the kind's action, as the run record holds it.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Shadow { .. } => "shadow",
        }
    }

    /// A shadow probe's first steady decision; `None` if its rule never stopped steady, and for
    /// a probe that is not a shadow probe.
    #[must_use]
    pub const fn would_stop(&self) -> Option<&Decision> {
        match self {
            Self::Probe => None,
            Self::Shadow { would_stop } => would_stop.as_ref(),
        }
    }
}

impl From<ProbeKind> for ProbedKind {
    /// The kind a probe of `kind` starts as, having recorded nothing.
    fn from(kind: ProbeKind) -> Self {
        match kind {
            ProbeKind::Probe => Self::Probe,
            ProbeKind::Shadow => Self::Shadow { would_stop: None },
        }
    }
}

/// The decision a shadow probe with `settings` records after `samples`, taking the rule as
/// [`decide`] does but never acting on it: the decision, if it stops steady. `None` otherwise.
///
/// The maximum duration stops nothing, so past it the rule may still stop steady. The first
/// partition end closes the window, and a probe would have completed there, so after it no
/// decision is steady. The first decision a shadow probe records is when, and with what estimate,
/// a probe would have stopped.
#[must_use]
pub fn would_stop(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> Option<Decision> {
    if first_partition_end_ns.is_some() {
        return None;
    }
    // Without a cap, the rule takes no capped estimate only for it to be discarded, and the one
    // decision left to it is steady.
    let uncapped = ProbeSettings {
        max_duration: Duration::MAX,
        ..settings.clone()
    };
    decide(&uncapped, samples, None)
}

/// The stopping rule's judgement at the end of a probe batch: where warmup ends over the samples
/// up to it, and how tight the estimate interval over the window after that is.
///
/// A check depends on the batch duration and the window groups alone, so one series of checks
/// serves every precision, number of consecutive checks and minimum duration.
#[derive(Clone, Debug, PartialEq)]
pub struct TightnessCheck {
    /// The elapsed nanoseconds of the sample that ends the batch.
    pub elapsed_ns: u64,
    /// The index of that sample among the probe's progress samples.
    pub sample_index: usize,
    /// The elapsed nanoseconds of the sample that ends warmup and starts the window; `None` when
    /// MSER found no end of warmup, and the window starts at the first sample.
    pub warmup_end_ns: Option<u64>,
    /// The rows received per second over the window; `None` when it spans no time.
    pub steady_state_throughput: Option<f64>,
    /// The half-width of the window's estimate interval over its mean; `None` when the window
    /// has no interval, or its mean is not positive.
    pub relative_half_width: Option<f64>,
}

impl TightnessCheck {
    /// Whether the check passes at `precision`: it found an end of warmup, and its relative
    /// half-width is below the precision.
    #[must_use]
    pub fn passes(&self, precision: f64) -> bool {
        self.warmup_end_ns.is_some()
            && self
                .relative_half_width
                .is_some_and(|half_width| half_width < precision)
    }
}

/// The tightness checks of a probe with `settings` over `samples`, one per probe batch end, in
/// order. Only the batch duration and the window groups of `settings` matter.
#[must_use]
pub fn tightness_checks(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
) -> Vec<TightnessCheck> {
    let ends = batch_ends(settings.batch_duration, samples);
    checks_from(settings, samples, &ends, 0)
}

/// The tightness check that stops a probe with `settings`, from its `checks` in order; `None` if
/// none does.
///
/// That is the first check at or past the minimum duration whose last `consecutive_checks`
/// checks, itself included, all passed at the precision. Only the precision, the consecutive
/// checks and the minimum duration of `settings` matter. `first_partition_end_ns` is the elapsed
/// nanoseconds of the first sample that showed a finished partition, if one has.
///
/// Passes before the minimum duration count toward a streak. Only checks before the sample that
/// showed the first partition end count, since a probe would have completed there. This takes
/// samples to have strictly increasing times, so that a check comes before that sample exactly
/// when its time is before `first_partition_end_ns`: the run record holds the partition end as a
/// time, not as a sample.
///
/// The probe viewer keeps a copy of this function as Vega-Lite transforms (#220), so that its
/// sliders move the stop without a replay; the ADR that lands with it records why. Change both
/// together. The shared fixture `python/tests/fixtures/stop_over_tightness_checks.json` holds
/// stops this function finds, which the copy is tested against, and a module test fails with the
/// regenerated fixture when it is stale.
#[must_use]
pub fn stop<'a>(
    settings: &ProbeSettings,
    checks: &'a [TightnessCheck],
    first_partition_end_ns: Option<u64>,
) -> Option<&'a TightnessCheck> {
    let consecutive = consecutive_checks(settings);
    let mut streak = 0_usize;
    checks
        .iter()
        .take_while(|check| first_partition_end_ns.is_none_or(|end_ns| check.elapsed_ns < end_ns))
        .find(|check| {
            streak = if check.passes(settings.precision) {
                streak.saturating_add(1)
            } else {
                0
            };
            streak >= consecutive && Duration::from_nanos(check.elapsed_ns) >= settings.min_duration
        })
}

/// The elapsed nanoseconds of the first of `samples` at or past the maximum duration of
/// `settings`, at which a probe with them is capped unless it stopped first; `None` if none is.
#[must_use]
pub fn capped_at_ns(settings: &ProbeSettings, samples: &[ProgressSample]) -> Option<u64> {
    samples
        .iter()
        .find(|sample| at_or_past_max_duration(settings, sample))
        .map(|sample| sample.elapsed_ns)
}

/// How a probe would have ended, in the stop reason vocabulary.
///
/// The arguments are the elapsed nanoseconds of the tightness check that stops it, of the first
/// sample at or past its maximum duration, and of the first sample that showed a finished
/// partition, each if there is one. The probe ends steady if a check stops it no later than the
/// cap. Otherwise it is capped if the cap comes before the first partition end. Otherwise it
/// completes, as it does when the cap and the first partition end are the same sample.
#[must_use]
pub fn probe_stop_reason(
    stop_ns: Option<u64>,
    capped_at_ns: Option<u64>,
    first_partition_end_ns: Option<u64>,
) -> StopReason {
    match (stop_ns, capped_at_ns) {
        (Some(stop_ns), capped_at_ns) if capped_at_ns.is_none_or(|capped| stop_ns <= capped) => {
            StopReason::Steady
        }
        (_, Some(capped_at_ns))
            if first_partition_end_ns.is_none_or(|end_ns| capped_at_ns < end_ns) =>
        {
            StopReason::Capped
        }
        _ => StopReason::Completed,
    }
}

/// Whether `sample` was taken at or past the maximum duration of `settings`.
fn at_or_past_max_duration(settings: &ProbeSettings, sample: &ProgressSample) -> bool {
    Duration::from_nanos(sample.elapsed_ns) >= settings.max_duration
}

/// The consecutive checks of `settings` as a count of checks.
fn consecutive_checks(settings: &ProbeSettings) -> usize {
    usize::try_from(settings.consecutive_checks.get()).unwrap_or(usize::MAX)
}

/// Whether the latest of `samples` ends a probe batch whose tightness check stops the probe.
fn steady(settings: &ProbeSettings, samples: &[ProgressSample]) -> bool {
    let ends = batch_ends(settings.batch_duration, samples);
    let consecutive = consecutive_checks(settings);
    // A probe judges after every sample, so rule out first, without building any check, a latest
    // sample that ends no batch, comes before the minimum duration, or ends too few batches.
    let latest = samples.len().checked_sub(1);
    if ends.last().copied() != latest
        || samples
            .last()
            .is_none_or(|sample| Duration::from_nanos(sample.elapsed_ns) < settings.min_duration)
        || ends.len() < consecutive
    {
        return false;
    }
    // Only the checks at the last `consecutive` batch ends can make up a streak that ends at the
    // latest one.
    let latest_checks = checks_from(
        settings,
        samples,
        &ends,
        ends.len().saturating_sub(consecutive),
    );
    stop(settings, &latest_checks, None).is_some_and(|check| Some(check.sample_index) == latest)
}

/// The tightness checks at the batch ends `ends` of `samples`, from the one at position `from`.
fn checks_from(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    ends: &[usize],
    from: usize,
) -> Vec<TightnessCheck> {
    (from..ends.len())
        .filter_map(|position| {
            let &end = ends.get(position)?;
            // The batch ends of the samples up to a batch end are those up to it.
            let window = Window::over(samples.get(..=end)?, ends.get(..=position)?)?;
            let (first, last) = (window.samples.first()?, window.samples.last()?);
            Some(TightnessCheck {
                elapsed_ns: last.elapsed_ns,
                sample_index: end,
                warmup_end_ns: window.warmup_end_ns,
                steady_state_throughput: rate_between(first, last),
                relative_half_width: window.relative_half_width(settings),
            })
        })
        .collect()
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
        Self::over(samples, &batch_ends(settings.batch_duration, samples))
    }

    /// [`Self::after_warmup`], given the batch ends of `samples`.
    fn over(samples: &'a [ProgressSample], ends: &[usize]) -> Option<Self> {
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

    /// The half-width of the window's estimate interval over its mean; `None` when there is no
    /// interval, or its mean is not positive.
    ///
    /// The window splits into `window_groups` groups of equal duration, their boundaries snapped
    /// to the nearest sample, and the interval is a 95% t-interval over the groups' rates. A group
    /// that snaps to no time gives no interval, as some must when there are as many groups as
    /// samples, which is checked first so that no group count costs more than the samples.
    fn relative_half_width(&self, settings: &ProbeSettings) -> Option<f64> {
        let (first, last) = (self.samples.first()?, self.samples.last()?);
        let groups = settings.window_groups;
        if usize::try_from(groups).map_or(true, |groups| groups >= self.samples.len()) {
            return None;
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
        relative_half_width(&rates?)
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

/// The half-width of a 95% t-interval for the mean of `rates` over that mean; `None` when the
/// mean is not positive or there are fewer than two rates.
fn relative_half_width(rates: &[f64]) -> Option<f64> {
    let freedom = rates.len().checked_sub(1).filter(|&freedom| freedom > 0)?;
    let (count, freedom) = (count_f64(rates.len()), count_f64(freedom));
    let mean = rates.iter().sum::<f64>() / count;
    let variance = rates.iter().map(|rate| (rate - mean).powi(2)).sum::<f64>() / freedom;
    let half_width = t_quantile(freedom) * (variance / count).sqrt();
    (mean > 0.0).then(|| half_width / mean)
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
