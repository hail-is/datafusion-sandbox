# Write one file per partition through a partitioned sink

The interval-merge formulation of the reference combiner merges every sample within each locus
interval and writes each interval's rows to its own file. Its plan builder returns one frame: the
union formulation's frame filtered by each interval, the `j` branches unioned. A write runs that
frame into a `PartitionedSinkExec`, the repo's first custom `ExecutionPlan`, which requires the
formulation's ordering of every input partition, places no distribution requirement, and runs
input partition `i` into a file sink of its own. The `j` interval merges execute concurrently
beneath the one sink, and the plan builder stays one `DataFrame` for every formulation. This is the
decision from [issue #140](https://github.com/hail-is/datafusion-sandbox/issues/140), where a
prototype settled it.

## Why `DataSinkExec` cannot write one file per partition

DataFusion's `DataSinkExec`, which every single-file write and the collecting and draining sinks
use, is built to write one stream. Traced against `datafusion-datasource-55.0.0/src/sink.rs`:

1. It requires `Distribution::SinglePartition` of its input, so the optimizer merges the `j`
   interval merges into one stream before the sink sees them.
2. `execute` asserts that the partition requested is 0 and executes only input partition 0.
3. Its output partitioning is one partition, one count batch.

Beneath it, the format's file sink demuxes the one stream across files by row count, round-robin
in whole batches, so file order matches row order only for a single file. One plan through
`DataSinkExec` therefore writes one ordered file. The map for this work had earlier ruled a custom
sink out; that ruling covered DataFusion's exec before the `sink` module and the sink-target
mechanism of [ADR 0014](0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md) existed.

## Decision

- **A partitioned sink exec.** `PartitionedSinkExec` reports `UnspecifiedDistribution`, requires
  the ordering for its one child, does not benefit from input partitioning, maintains input order,
  and reports one output partition per input partition with the count schema. Its `execute(i)`
  runs partition sink `i`, and its metrics are every partition sink's metrics together, so an
  analyzed explain reports the writes. It reaches the plan through a `SinkTarget`, so the ordering
  requirement arrives through the sink provider as it does for every other sink.
- **The format builds every partition sink itself.** Each partition sink is the plan the format's
  `create_writer_physical_plan` returns for a single-file configuration at `dir/<index>.<ext>`,
  the index zero-padded to the digit count of the partition count, over the input-partition plan
  the exec supplies: a one-partition plan yielding input partition `i`. Building through the
  format rather than the
  sink constructors keeps what the format adds beyond the constructor: Parquet's sorting-column
  metadata from the ordering, Vortex's compact encodings from the compression option, and the
  session's table options. In the pinned Vortex crate the compact-encoding setter is
  crate-private, so a sink built directly could not honor `--compression compact`; the format
  route is the only one that does.
- **The partition sinks are built once, at plan time.** The format needs the session to build a
  sink, and the session is available when the target is planned but not when the plan executes.
  The input at plan time is the pre-optimization plan, whose partition count is the interval count
  already: each interval's sort is one partition before optimization and one merge after it. The
  optimizer swaps its final input beneath every partition sink through `replace_children`. That
  call accepts an input with any partition count, because distribution enforcement probes the
  sink with a hypothetical coalesced child to compare pipeline behavior
  (`enforce_distribution.rs`, `preserving_order_enables_streaming`) and a refusal there fails the
  whole plan. Executing a sink whose input's partition count no longer matches its sinks fails
  with an internal error instead, rather than writing the wrong number of files.
- **Collect, drain, and explain keep `DataSinkExec`.** Its single-partition requirement puts one
  merge above the union of interval merges, so `--show` and the row tests see global locus order.
- **The formulation exposes its output layout**, one file or one file per partition. The run picks
  the sink target by it, and the CLI validates the write path by it: a path with an extension is
  rejected under interval-merge because it names the directory the files go into. `--limit` is
  rejected with interval-merge because a limit above the union forces one partition and one file,
  which would silently undo the parallelism; the library run keeps that defined one-file behavior.
  An empty interval writes an empty file, so the directory always holds one file per interval.

## What the prototype confirmed

A stub exec with these properties over the interval-merge frame, in both formats, both locus
representations, under the shared session and a hostile one with file-splitting allowed, with
three intervals and with an empty one: the plan is the sink over a union of one merge per interval,
each over every sample's scan with the interval's predicate pushed into the scan and its files
pruned by it. No `CoalescePartitionsExec`, `SortExec`, `RepartitionExec`, or `FilterExec` appears.
In the terms of ADR 0014: a union whose children all report the ordering reports it and maintains
input order, so sort enforcement keeps the branch sorts and turns each into a merge, and with an
unspecified distribution and no benefit from partitioning, distribution enforcement adds neither a
coalesce nor a round-robin above the union. The empty interval keeps a straddling file in every
scan and yields a `count=0` batch, not an `EmptyExec`.

## Considered options

- **`j` independent single-file writes** spawned and folded by the run, the first plan for this
  ticket. It made the plan builder return `j` frames for one formulation, gave explain `j` plans,
  and relisted every sample per write.
- **Sinks built from `ParquetSink::new` and `VortexSink::new`** through a per-partition
  constructor the format supplies, so the exec could build a sink for any partition count at
  execute time. Rejected because Vortex's compact-encoding setter is crate-private, and it would
  have reimplemented the format factories' option and sorting-column handling.
- **Input-partition plans under `DataSinkExec` without a partitioned exec**, one `DataSinkExec`
  per interval. Each would still require a single partition of its input and there would be `j`
  roots; nothing holds them in one plan.

## Consequences

Interval-merge plan-shape tests observe the plan through the partitioned sink for writes and
through the collecting sink for rows, and the write tests read the files back in index order. The
exec depends on the same 55.0.0 enforcement behavior ADR 0014 records, traced there and on issue
#139; the plan-shape tests are what would notice an upstream change. A partition count that
changes during optimization is an internal error at execution by design; the plan-shape tests,
which check the sink's partition count, would notice it before then.
