//! A throughput probe's settings, its progress samples, and the decision to stop it.
//!
//! A throughput probe runs a formulation's plan, takes a [`ProgressSample`] every poll period,
//! and stops once [`decide`] says so. Stopping and sampling belong to [`crate::sink::probe`];
//! this module only judges the samples, so the decision is a pure function of the settings, the
//! samples so far, and the first partition end, and a recorded probe replays exactly.
//!
//! The decision stops only at the maximum duration or at the first partition end, and estimates
//! over every sample taken: there is no warmup cut-off yet. See
//! [ADR 0017](../docs/adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).

use std::time::Duration;

/// How a throughput probe samples and when it gives up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeSettings {
    /// The time between progress samples.
    pub poll_period: Duration,
    /// The execution time after which the probe stops, capped, whatever its samples show.
    pub max_duration: Duration,
}

impl Default for ProbeSettings {
    fn default() -> Self {
        Self {
            poll_period: Duration::from_millis(100),
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
    /// The elapsed nanoseconds of the window's last sample.
    pub window_end_ns: u64,
    /// The rows the sink received between the window's end samples.
    pub window_rows: u64,
}

/// Whether a probe with `settings` stops after `samples`, in the order taken, given the elapsed
/// nanoseconds of the first sample that showed a finished partition, if one has. `None` keeps
/// it running.
///
/// The probe completes at the first partition end, and is capped once its latest sample is at
/// or past the maximum duration. The measurement window runs from the first sample to the latest
/// one, or to the first partition end if one has been seen.
#[must_use]
pub fn decide(
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> Option<Decision> {
    let (first, latest) = (samples.first()?, samples.last()?);
    let (stop_reason, window_end) = match first_partition_end_ns {
        Some(end_ns) => (
            StopReason::Completed,
            samples
                .iter()
                .take_while(|sample| sample.elapsed_ns <= end_ns)
                .last()?,
        ),
        None if Duration::from_nanos(latest.elapsed_ns) >= settings.max_duration => {
            (StopReason::Capped, latest)
        }
        None => return None,
    };
    let window_rows = window_end.rows.saturating_sub(first.rows);
    let window_ns = window_end.elapsed_ns.saturating_sub(first.elapsed_ns);
    Some(Decision {
        stop_reason,
        steady_state_throughput: (window_ns > 0).then(|| rate(window_rows, window_ns)),
        window_end_ns: window_end.elapsed_ns,
        window_rows,
    })
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
