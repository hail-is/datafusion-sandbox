# datafusion-sandbox

Prototypes of Hail-style genomics pipelines built on DataFusion, and the benchmarks that time them.
The point of the repo is comparing how different formulations of the same query plan perform, so the
shape of a plan is itself a first-class subject here, not just its results.

## Language

### Pipelines

**Pipeline**:
A named end-to-end query over genomics data, from input tables through to its consumed result. It
runs to completion; its result is never deferred work.
_Avoid_: job, query (too narrow), driver

**Combiner run**:
One execution of a combiner against a dataset, from resolved settings through to an Outcome.
_Avoid_: invocation, command

**Action**:
What a caller asks a combiner run to do with its combined rows: write them, collect them for display,
render the plan, or execute and render the analyzed plan. The Action determines the run's Outcome.
_Avoid_: mode (taken by **Compression mode**), ending, sink (the operator the rows end in, which
the action chooses; a separate term)

**Outcome**:
What a pipeline hands back after it runs: rows written, collected batches, or a plan rendered as
text.
_Avoid_: result (too broad), output (ambiguous with a written artifact)

**Sink**:
The operator a combiner run's rows end in, and the plan's only consumer: a file sink when the
action writes, a partitioned file sink when it writes one file per locus interval, a collecting
sink when it collects, a draining sink when it only analyzes. The action chooses the sink; the sink
requires the combiner's ordering. See
[ADR 0014](docs/adr/0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md) and
[ADR 0015](docs/adr/0015-write-one-file-per-partition-through-a-partitioned-sink.md).
_Avoid_: consumer, writer, terminal, output

**Combiner**:
A merge of per-sample tables into a single locus-ordered table, named by the table it produces. A
combiner says what is produced, not how; the reference combiner and the allele combiner are the
two we have.
_Avoid_: merger, joiner

**Formulation**:
One way of building a combiner's plan. Two formulations of the same combiner return the same rows
and differ in plan shape; comparing them is the point of the repo.
_Avoid_: variant, strategy, combiner (a formulation is not itself a combiner)

**Plan builder**:
The part of a formulation that constructs its ordered frame over a dataset and stops. It does no
runtime setup, writing, or execution.
_Avoid_: query builder, factory

**Ordered frame**:
What a plan builder returns: the combiner's rows as deferred work, paired with the stored ordering
a sink must require of them and the output layout a write gives them. A run sinks an ordered frame,
never a bare frame.
_Avoid_: frame (unqualified), sorted frame (a sort is one way to satisfy the ordering; a merge tree
is another), combined table

**Output layout**:
How a write lays a formulation's rows out at the output path: one file, or one file per partition of
its frame. The formulation chooses it; the action does not.
_Avoid_: layout (unqualified), partitioning, write mode

**Output path**:
The path a write is given. Under a one-file output layout it names the file; under a
file-per-partition layout it names the directory the partition files go into.
_Avoid_: output (ambiguous with Outcome), destination, write path

**Session**:
The DataFusion configuration and state a plan is built against.
_Avoid_: context, config (either alone is narrower than what plan shape depends on)

**Plan shape**:
The structure of the physical plan a plan builder produces: which operators appear and how they
nest. Distinct from the plan's results; two plan shapes can be equivalent in output and differ by
an order of magnitude in time.
_Avoid_: query plan (ambiguous between logical and physical), execution graph

### Runtimes

**CPU runtime**:
The Tokio runtime a pipeline's plan executes on. Separate from the IO runtime so that plan
execution and object store requests do not compete for the same threads. See
[ADR 0006](docs/adr/0006-run-every-plan-through-the-pipeline-runner.md).
_Avoid_: worker pool, executor, thread pool

**IO runtime**:
The Tokio runtime object store requests run on.
_Avoid_: network runtime, blocking pool (Tokio's own, separate, thing)

### Data

**Sample**:
One sequenced individual, identified by a string id such as `HG00308`.
_Avoid_: individual, subject

**Sample set**:
Which samples a combiner run covers: a nonempty property of the dataset, which a caller may narrow
but neither empty nor extend.
_Avoid_: samples (unqualified), sample list, cohort

**Split point**:
A locus at which one locus interval ends and the next begins. `j - 1` split points, strictly
increasing in the locus ordering, define `j` locus intervals.
_Avoid_: boundary, breakpoint, cut point, partition key

**Locus interval**:
A contiguous stretch of loci in the dataset's locus ordering, half-open: it includes its start and
excludes its end. The locus intervals of a run partition the whole ordering, so every row falls in
exactly one; a plan that merges by locus interval merges every sample within each interval and
writes each interval's rows to its own file.
_Avoid_: range, region, vertical partition, genome partition

**Sample group**:
A subset of a combiner run's sample set whose per-sample tables one merge node combines. The
sample groups of a run partition its sample set; a plan that merges by sample group merges the
groups again afterwards.
_Avoid_: batch, shard, horizontal partition

**Dataset**:
One stored collection of per-sample tables under a declared locus ordering, identified by its path,
the format of each file, and the sample set found there. A dataset without a declared locus
ordering does not exist.
_Avoid_: input, table (a dataset holds many per-sample tables), corpus, dataset layout (retired)

**Generated table**:
A table whose rows a generator produces on demand rather than reading them from storage. It has no
encoded form and no files, so it cannot exercise listing, schema inference, or file statistics, and
it declares its own sort order rather than recovering one from statistics.
_Avoid_: synthetic table, mock table, fake data

**Dataset fixture**:
A small dataset written by a test, encoded in a real format, and held in an object store (backed by
cloud storage, local disk, or memory) as a stand-in for a genomics dataset a run would read. Unlike
a generated table, it is encoded and stored.
_Avoid_: test data, sample data, fixture (unqualified)

**Metadata-only table**:
A sorted table whose files exist only as declared metadata, a path, a size, and ordering
statistics, with no stored bytes behind them, so it can be planned but never scanned. Unlike a
generated table it has files, and unlike a dataset fixture nothing is encoded.
_Avoid_: fake files, stub table, mock store, supplied-statistics table

**Sorted table**:
A file-backed table whose files are taken to hold one sorted table, scanned as one ordered
partition in the file order its ordering statistics recover under that assumption. It has no
knowledge of samples, contigs, or genomics. See
[ADR 0011](docs/adr/0011-recover-file-order-instead-of-proving-it.md) for what the assumption
trusts.
_Avoid_: listing table, sample table, sorted scan

**File pruning**:
Leaving out of a scan every file whose ordering statistics show that no row in it can satisfy a
filter. It removes files from a sorted table's order without disturbing it.
_Avoid_: partition pruning, file skipping, statistics pruning

**Filter pushdown**:
Applying a filter inside the scan that reads its rows rather than in an operator above it. A pushed
filter is exact when the scan applies all of it and inexact when an operator above must apply it
again.
_Avoid_: predicate pushdown, pushdown unqualified

**Ordering statistics**:
The per-file minimum and maximum of each stored ordering field. The only evidence a sorted table
has of where a file's rows fall.
_Avoid_: file statistics (broader), min/max, pruning statistics

**Locus**:
A position in the genome: a contig together with a position within it. The unit both combiners
order and group by.
_Avoid_: site, coordinate, variant (a variant is a locus plus alleles)

**Contig ordinal**:
The integer standing for a contig, which names it as `chr{ordinal}` and places it in the locus
ordering. The packed representation stores loci by it; a caller names split points by it.
_Avoid_: contig index, chromosome number, contig id

**Locus representation**:
How one stored row records its locus. `contig-position` uses separate `contig` and `position`
fields; `packed` uses one `Int64` `locus` field.
_Avoid_: encoding (ambiguous next to compression and Vortex encoding sets), locus format

**Locus ordering**:
A sequence of locus components, the locus optionally followed by alleles, declaring the sort order
that makes a plan mergeable rather than re-sorted. It names no stored field, so one ordering is
declarable against either locus representation, and a finer ordering satisfies a coarser
requirement.
_Avoid_: sort key, ordering (unqualified), stored ordering (the expansion, a separate term)

**Stored ordering**:
The expansion of a locus ordering into stored fields under a dataset's locus representation:
`contig` then `position`, or the packed `locus` field, optionally followed by `alleles`.
_Avoid_: sort expressions, physical ordering, locus ordering (the declaration, a separate term)

**Reference data**:
The per-sample records covering loci where a sample matches the reference genome, stored as
runs of adjacent loci rather than one record each. Input to the reference combiner.
_Avoid_: ref blocks, non-variant data

### Formats

**Format**:
The encoding of one file, together with how to read or write it, its extension, and the compression
modes it accepts. It does not describe how a dataset arranges rows across files; that is the
dataset's locus ordering and its output layout.
_Avoid_: locus ordering, output layout, codec, encoding

### Benchmark settings

**Compression mode**:
A compression choice, always relative to an output format: a codec and level for Parquet, or an
encoding scheme set for Vortex.
_Avoid_: compression codec (too narrow for Vortex), encoding (ambiguous without a format)

**Thread count**:
The number of worker threads given to both of a pipeline's runtimes, defaulting to available
parallelism. Independent of the session's target-partition setting.
_Avoid_: parallelism, cores, degree of parallelism
