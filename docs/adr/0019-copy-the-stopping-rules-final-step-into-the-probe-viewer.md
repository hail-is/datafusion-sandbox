# Copy the stopping rule's final step into the probe viewer

The probe viewer shows how the stopping rule would have judged each shadow probe under other
settings, with sliders for precision, consecutive checks and minimum duration that move the stop,
the would-be estimate and the calibration headline live. The page is static and opened from a
file, so it cannot call Rust, and a grid of precomputed stops fine enough to feel continuous
would inline tens of thousands of rows. We copy the rule's last, cheap step into the page instead.
[Issue #206](https://github.com/hail-is/datafusion-sandbox/issues/206) had forbidden any copy of
the rule outside Rust; this reverses that for the final step only. Decided in a grilling session
on 2026-09-30.

## Decision

- **Rust owns the numerics.** Replay writes, for each shadow probe and each pair of batch duration
  and window groups, one row per tightness check: its time, the end of warmup if MSER found one,
  the steady-state throughput and the estimate interval's relative half-width. Probe batches, MSER
  and the t-interval exist only in Rust.
- **The page owns only the stop over those tightness checks.** A tightness check passes if warmup
  has ended and its relative half-width is strictly below the precision. The stop is the first
  tightness check at or past the minimum duration whose last `consecutive_checks` tightness checks,
  itself included, all passed, among the tightness checks before the sample that showed the first
  partition end. Passes before the minimum duration count toward the streak. A probe with no stop
  at or before the first progress sample at or past the maximum duration is capped there, unless
  its first partition end comes no later, when it completes; Rust supplies that sample's time.
- **A fixture keeps the two in step.** Rust writes a small set of tightness checks with the stops
  its own rule finds under several settings, covering each edge above, and a Rust test fails if
  the checked-in fixture is stale. A Python test evaluates the page's transforms over the fixture
  with VegaFusion and compares. The Rust stop function and the page's transform each name the
  other and the fixture.
- **The overview uses the real rule.** The chart of every settings combination, which is what a
  tuning decision is read from, takes its stops from Rust, not from the page's copy.

## Considered options

- **Visual inspection only**: draw the passing tightness checks and let the reader count streaks.
  No copy, but the headline could not follow the sliders.
- **Sliders snapping to precomputed stops**: no copy and no drift, but a grid fine enough for the
  sliders costs about 80,000 stop rows inlined into the page, for steps that are still discrete.
