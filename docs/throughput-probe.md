# Throughput probes

A throughput probe, `--probe --metrics DIR`, runs the formulation's unchanged plan, drains its
rows, or writes them as described [below](#writing-probes), and stops early to estimate the plan's
steady-state throughput without running it to the end. It applies to the reference combiner's
[combiner runs](combiner.md). Before relying on the stopping rule in a sweep,
[calibrate it](probe-calibration.md).

## Stopping rule

Every `--poll-period` it takes a progress sample: the
time since execution started and the rows the sink has received. After each progress sample it
applies a stopping rule, a pure function of its settings, the samples so far and the first
partition end:
1. It batches the samples into batches spanning at least `--batch` each, and takes each batch's
   rate as its rows over the time between its end samples.
2. It picks the end of warmup with MSER: the number of batches d that minimizes
   Σ_{i>d} (Yᵢ − Ȳ_d)² / (n − d)² over the batch rates Yᵢ. Only cuts that leave at least 5 batches
   count, since the variance of the last few rates is too noisy to compare. If the minimum lies
   past n/2, the run is still too short to judge, and the probe keeps running.
3. Its measurement window runs from the end of warmup to the latest sample, or to the first
   partition end.
4. At each batch end, it checks whether the window is tight: split into `--window-groups` groups
   of equal duration, with boundaries snapped to the nearest sample, the group rates' 95%
   t-interval, the estimate interval, has a half-width below `--precision` of its mean.
5. It stops with stop reason `steady` at a batch end past `--min-duration` once the last
   `--consecutive` checks were all tight.

It also stops at the first finished partition of the plan, with stop reason `completed`, or once
`--max-duration` has passed, with stop reason `capped`, unless the same sample stops it steady.
Whatever the reason, its steady-state throughput is the rows received between the two end samples
of its measurement window over the time between them, and when MSER has found no end of warmup,
the window starts at the first sample. It prints the run id, the rows received, that throughput,
and the stop reason.

## Settings

Each setting requires `--probe`:

| Flag | Default | Meaning |
|---|---|---|
| `--poll-period SECONDS` | 0.1 | the time between progress samples |
| `--batch SECONDS` | 1 | the least time a batch spans |
| `--precision FRACTION` | 0.02 | the relative half-width below which a check is tight |
| `--consecutive COUNT` | 3 | the tight checks in a row that stop the probe |
| `--window-groups COUNT` | 10 | the groups, 2 to 1000, the window splits into for its interval |
| `--min-duration SECONDS` | 20 | the execution time before which it does not stop steady |
| `--max-duration SECONDS` | 300 | the execution time at which it stops, capped |

Durations are in decimal seconds. The window's group count is not `--groups`, which grouped-merge
takes.

## Records

A probe records three tables under `DIR`, all Parquet and one file per run: the run record in
`DIR/runs/<id>.parquet`, the run metrics as they stood at the stop in `DIR/metrics/<id>.parquet`,
and its progress samples, one row each of run id, sample index, elapsed nanoseconds, and rows, in
`DIR/progress/<id>.parquet`. Replaying a shadow probe adds three more tables, again one file per
run: its tightness checks in `DIR/checks/<id>.parquet`, its end-of-run decisions in
`DIR/replay-baselines/<id>.parquet`, and its stops in `DIR/replays/<id>.parquet`; see
[Replay](probe-calibration.md#replay). A probe is recorded only once it succeeds, so its run record's
`status` is always `completed`; one that fails or is cancelled records nothing. Its run record holds the rows received as `rows_written`, times
`execute_ns` to the stop, and fills the probe columns a measured write leaves empty: `action`,
`stop_reason`, `steady_state_throughput`, `relative_half_width`, `warmup_end_ns`, `window_end_ns`,
`window_rows`, `first_partition_end_ns`, and every setting, as `poll_period_ns`,
`batch_duration_ns`, `precision`, `consecutive_checks`, `window_groups`, `min_duration_ns`, and
`max_duration_ns`. `relative_half_width` is the estimate interval's half-width over its mean at
the decision the probe stopped with, whatever its stop reason: the value a tightness check of its
measurement window compares with `--precision`. It is empty when there is no estimate interval,
because the window has no more samples than `--window-groups` or a group spans no time, and when
the interval's mean is not positive. `warmup_end_ns` is empty when MSER found no end of warmup,
and `first_partition_end_ns` when no partition finished before the stop. `--run-id` works as for
a [measured write](combiner.md#measured-writes), a repeated id is refused before anything runs,
and a probe that fails records nothing. `--probe` refuses `--limit`, which would change the plan
measured. See [ADR 0017](adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --probe --metrics data/runs --run-id grouped-8-probe --max-duration 60
```

## Writing probes

A writing probe, `--probe --write PATH --metrics DIR`, writes the rows through the sink a plain
write to `PATH` would use, one file or a directory of one file per interval, with the output
format and `--compression` a plain write takes, so encoding and writing count in its throughput.
It samples the rows reaching the sink, as the draining form does; under interval-merge's
file-per-interval sink, that is the rows reaching all the interval writers, and the first interval
to finish closes the window. Its run record also fills `output_path`, `output_format`, and
`compression`. The output of a stopped write is incomplete, so a probe keeps none of it: after
every ending, whether steady, completed, capped, or failed, it removes everything under `PATH`,
and a local directory it created at `PATH`. So that the removal only ever touches what the probe
wrote, a probe refuses a `PATH` that already exists, as a file, as a non-empty directory, or,
on local disk, as a symlink of any kind, before it discovers the dataset.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation interval-merge --probe --write data/probe-out --metrics data/runs --run-id interval-probe --max-duration 60
```

On Google Cloud Storage, the removal does not reach unfinished multipart uploads. A write the
probe stops leaves one for each file it had not finished: every such Vortex file, whose writer
always uploads in parts, and every such Parquet file past its first 10 MiB. They hold no object
at `PATH` and no listing shows them, but their uploaded parts are billed until they are aborted,
and object_store can neither list nor abort them once the writer is gone. Probe into a bucket
with a lifecycle rule that aborts incomplete multipart uploads, for example after one day. This
command replaces the bucket's lifecycle configuration, so merge any rules it already has into
the file first:
```
echo '{"rule":[{"action":{"type":"AbortIncompleteMultipartUpload"},"condition":{"age":1}}]}' > lifecycle.json
gcloud storage buckets update gs://BUCKET --lifecycle-file=lifecycle.json
```

## Shadow probes

A shadow probe, `--probe --shadow`, drained or written, applies the stopping rule after each
progress sample exactly as a probe does, but never acts on it: it ignores `--max-duration` and
the first partition end, runs the plan to completion, and always stops with stop reason
`completed`. Its measurement window still closes at the first partition end, and its
`steady_state_throughput` and `relative_half_width` are the rule's estimate and estimate interval
at the end of the run. Its run record has `action` `shadow`, and records the first `steady`
decision the rule reached before the first partition end, the decision a probe would have stopped
at, in four more columns: `would_stop_ns`, the elapsed nanoseconds at which it would have stopped,
`would_be_steady_state_throughput`, `would_be_relative_half_width`, the relative half-width of the
estimate interval at that decision, and `would_be_warmup_end_ns`. Since the cap stops nothing,
`would_stop_ns` may lie past `--max-duration`, where a probe would have stopped capped instead.
They are empty when the rule never stopped steady, and for every other run.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --probe --shadow --metrics data/runs --run-id grouped-8-shadow
```
Shadow probes are the raw material for [calibrating the stopping rule](probe-calibration.md).
