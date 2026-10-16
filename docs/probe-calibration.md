# Calibrating the stopping rule

Calibrate the [throughput probe](throughput-probe.md)'s stopping rule with
[shadow probes](throughput-probe.md#shadow-probes) before relying on it in a sweep:

1. Run shadow probes on a few extreme configurations: the smallest and the largest branching
   factor, one thread and all threads, and interval-merge with many intervals.
2. Compare each run's would-be steady-state throughput and its estimate interval,
   `would_be_steady_state_throughput` and `would_be_relative_half_width`, with its steady-state
   throughput at the end of the run and its estimate interval, `steady_state_throughput` and
   `relative_half_width`, the estimate over the longest measurement window the dataset allows. An
   empty `would_stop_ns` means the rule never settled on that configuration.
   `uv run --directory python hailtools probe-viewer DIR [-o PATH]` draws this comparison for every
   shadow probe in the metrics directory `DIR`, and counts how many would-be estimate intervals
   cover the end-of-run estimate; see [Probe viewer](#probe-viewer).
3. Check that the work per row holds steady along the genome: the rate between progress samples
   should not drift over the run, since the estimate stands for the loci a probe does not reach
   only if it doesn't. The probe viewer's detail chart for each shadow run draws its batch rates
   over the run, with the stopping rule's decisions and the running estimate on them.
4. Tune the stopping rule by replaying the shadow probes with `cargo run -r -- replay DIR`, which
   judges every shadow probe in `DIR` again under a grid of settings without rerunning a sweep
   (see [Replay](#replay)), then pick settings in the probe viewer's
   [overview](#overview) and check them against each run's detail chart.

## Replay

The shadow probes' progress samples' row counts are cumulative and the rule is a pure function of
the samples and its settings, so `cargo run -r -- replay DIR` judges every shadow probe in `DIR`
again under a grid of settings without rerunning a sweep. The grid holds every combination of a
batch duration of 0.5, 1, 2 or 5 s, 5, 10, 20 or 40 window groups, a precision of 0.01, 0.02 or
0.05, 1, 3, 5 or 10 consecutive checks, and a minimum duration of 10, 20 or 60 s, with each run's
own poll period and maximum duration. A run leaves out every pair of batch duration and window
groups whose batch duration is shorter than its poll period. `replay` skips every other run and
writes three tables, one Parquet file per shadow probe, replacing any an earlier replay wrote. It
then deletes the tables' files of any run that is no longer a shadow probe in `DIR`, such as one
whose run record was removed, and prints how many runs it replayed, skipped and removed:
- `DIR/checks/<id>.parquet`, one row per tightness check under each pair: `batch_duration_ns`,
  `window_groups`, `check_index`, `sample_index`, `elapsed_ns`, and the check's
  `warmup_end_ns`, `steady_state_throughput`, and `relative_half_width`.
- `DIR/replay-baselines/<id>.parquet`, one row per pair: the end-of-run decision under it, as
  the run record's `steady_state_throughput`, `relative_half_width`, `warmup_end_ns`, and
  `window_end_ns` hold it, and `capped_at_ns`, the time of the first progress sample at or
  past the maximum duration.
- `DIR/replays/<id>.parquet`, one row per combination of the five settings: `would_stop_ns`
  and the `would_be_` columns as a shadow probe with it would have recorded them, and
  `probe_stop_reason`, how a probe with it would have ended. It is `steady` if the probe would
  have stopped steady no later than the cap. It is `capped` if the cap comes first and before
  the first partition end. Otherwise it is `completed`.

At a shadow probe's recorded settings, its replay reproduces what it recorded.
```
cargo run -r -- replay data/runs
```

## Probe viewer

`uv run --directory python hailtools probe-viewer DIR [-o PATH]` writes one self-contained page,
`DIR/probe-viewer.html` by default, that opens from a file and loads its chart scripts from a CDN.
A relative `DIR` is under `data/`.

The probe viewer reads the [replay](#replay) tables when `DIR` has them; without them it draws each
run under its recorded settings only, with a note naming the `replay` command. A control bar that
stays at the top of the page holds sliders for the precision, the consecutive checks and the
minimum duration, over the range the tables hold, and shows the selected batch duration and window
groups. The page opens on the settings every shadow probe was recorded with if they share them
and the grid holds them, and on the probe's default settings otherwise. Under each detail
chart, a tightness check panel draws every tightness check's relative half-width against the
precision: passing tightness checks in green, those that found no end of warmup hollow, the
minimum duration, and a thin strip of the end of warmup each found. The page finds each run's
stop over its tightness checks with a copy of the stopping rule's last step
([ADR 0019](adr/0019-copy-the-stopping-rules-final-step-into-the-probe-viewer.md)), so the
stop marker, the would-be estimate and its estimate interval follow the sliders, and the panel
says whether a probe would have stopped steady, been capped or completed. The detail chart's
batch rates are drawn at the selected batch duration.

### Overview

Above the headline, an overview draws every combination of the grid as one point, judged by
`replay` with the stopping rule itself rather than the page's copy. A point's x is the median
`would_stop_ns` over the runs a probe with it would have stopped steady, its y the worst
relative error of their would-be steady-state throughput against the end-of-run one under its
batch duration and window groups, from `replay-baselines`, and its colour the count of runs a
probe with it would have capped or completed. A dashed line joins the Pareto frontier, the
combinations no other is both faster and more accurate than, and a diamond marks the
combination the shadow probes were recorded with. A combination under which no run stops
steady has no median or error, so it is listed by its settings under the points. Hovering
over a combination gives its settings, how many of the runs it replayed stopped steady, how
many would-be estimate intervals cover the end-of-run estimate, its median stop and worst
error, and the runs it could not replay. Clicking it selects it: the pair moves to its batch
duration and window groups, the sliders move to its precision, consecutive checks and minimum
duration, and the headline and every detail chart follow. The overview outlines the selected
combination while the sliders sit on the grid's values, and the selected pair's combinations
while they sit between them. The headline draws the page's stop over each run's tightness
checks under the selected pair against the pair's end-of-run estimate. It draws a run that
settles only after a probe would have been capped hollow, and lists the runs that never
settled with those a probe would have capped apart. Under the recorded settings it matches the
headline of the recorded decisions.
