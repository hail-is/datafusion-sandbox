# Record run metrics as wide Parquet tables, one file per run

A measured write records how a run went in two tables under a metrics directory. The run record
holds one row per run: its resolved settings, the rows it wrote, its wall-clock timings, and the
process's peak resident set size. The run metrics hold one row per plan operator per partition,
with a column per metric DataFusion recorded. Each run adds `runs/<id>.parquet` and
`metrics/<id>.parquet`. The tables are always Parquet, each run writes its own file, and a metric
the schema has no column for is dropped with a warning rather than failing the run. This is the
decision from [issue #168](https://github.com/hail-is/datafusion-sandbox/issues/168), decided in a
grilling session on 2026-09-18.

The metrics come from the executed plan itself. The run builds the sink frame's physical plan,
executes it to completion, and keeps the plan, then walks the tree and reads each operator's
metrics. No analyze operator is inserted, so the plan measured is the plan a plain write executes,
and explain-analyze is left as it was.

## Decision

- **Wide rows, one column per metric.** A query such as "elapsed compute per merge node" is a
  column reference, not a pivot, and the schema states which metrics exist. The cost is that a
  metric with a name the schema does not know has nowhere to go. Rejected: long rows of
  `(run, node, partition, metric, value)`. They accept any metric, and every query pivots first.
- **Parquet always, whatever the output format.** The analysis tools read Parquet, and the format
  under test should not be a dependency of measuring it: a Vortex encoding that is slow or broken
  must not slow or corrupt the record of that run. Rejected: the output format, which would keep
  a run's files uniform at the price of tying the measurement to the thing measured.
- **A directory of one file per run.** Neither DataFusion nor the Vortex writer appends rows to
  an existing file; DataFusion's append adds a file to a directory-backed table. One file per run
  is that append, and a run never touches another run's file. Any Parquet reader reads
  `runs/` or `metrics/` as one table. Rejected: rewriting a single table per run, which reads the
  history to append to it and loses it on a failure mid-write.
- **Warn and drop an unknown metric.** By the time the plan's metrics are read, the data write has
  succeeded. A metric the schema does not know is dropped, its name is handed back in the
  outcome, whose rendering carries one warning line per name; the tables are written. Rejected:
  failing the run,
  which would turn a long write into a wasted one whenever a DataFusion upgrade adds a metric,
  and which would also lose the rows that were recorded.

## What is not recorded

- **The DataFusion memory pool's peak.** The pipeline configures no pool, and scans and merge trees
  reserve nothing from one, so the peak would be near zero for three of the four formulations and
  would say nothing about scan buffers. Process peak resident set size is the measurement that
  sees those, and the run record's `peak_rss_bytes` carries it (issue #173). It is the process's
  lifetime high-water mark as of the plan's completion, which for the CLI's one run per process is
  the run's peak, and it counts the pages the allocator keeps resident, so it reflects the
  allocator in use, including `snmalloc` when that feature is on.
- **Vortex's own scan metrics.** Bytes read and decode time live in Vortex's registry and reach a
  plan only through a helper this repo does not call, so a Vortex scan's row holds the file-stream
  metrics and nothing Vortex-specific. A near-empty Vortex scan row is not a cheap scan. Issue
  #169 tracks recording them.

## Consequences

Adding a metric means adding a column, and until it is added every run that reports the metric
warns. The tracer bullet in issue #171 records the six baseline metrics and issue #172 adds the
rest. The `run_metrics` module owns both schemas and the tree walk and knows nothing of datasets
or storage, so its tests run against a generated-table plan in memory, per
[ADR 0010](0010-keep-tests-on-in-memory-object-stores.md).

Two guarantees protect the history a metrics directory accumulates (issue #174). A measured write
refuses a run id that already has a run record under the directory, before it discovers the
dataset or writes anything: a single-file write silently replaces an existing object, so the check
is the only thing standing between a repeated id and an overwritten run. And a measured write that
fails records nothing: the tables are written only after the data write succeeds, and the run
record is written last, so it is the mark of a recorded run. A failure between the two table
writes leaves run metrics without a record, and a retry of that id replaces them rather than being
refused.
