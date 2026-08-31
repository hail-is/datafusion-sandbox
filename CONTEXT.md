# datafusion-sandbox

Prototypes of Hail-style genomics pipelines built on DataFusion, and the benchmarks that time them.
The point of the repo is comparing how different formulations of the same query plan perform, so the
shape of a plan is itself a first-class subject here, not just its results.

## Language

### Pipelines

**Pipeline**:
A named end-to-end query over genomics data, from input tables through to its consumed result. A
pipeline is the closure the runner executes: it takes a session, runs to completion on the
pipeline's runtimes, and returns what it produced to the calling thread. A combiner run returns an
Outcome; fixture writers and tests also construct pipelines with other result types. A pipeline
result is not a DataFrame tied to work that has yet to execute.
_Avoid_: job, query (too narrow — a pipeline includes its execution setup), driver

**Combiner run**:
One execution of a combiner against a dataset, from resolved settings through to an Outcome.
_Avoid_: invocation, command

**Action**:
What a caller asks a combiner run to do with its combined rows: write them, collect them for display,
render the plan, or execute and render the analyzed plan. The Action determines the run's Outcome.
_Avoid_: mode (taken by **Compression mode**), ending, sink

**Outcome**:
What a pipeline hands back after it runs: rows written, collected batches, or a plan rendered as
text. Both plain explain and explain-analyze produce the rendered-plan case.
_Avoid_: result (too broad), output (ambiguous with a written artifact)

**Combiner**:
A merge of per-sample tables into a single locus-ordered table, named by the table it produces.
The reference combiner and the allele combiner are the two we have. A combiner says what is
produced, not how; each is realized by one or more formulations.
_Avoid_: merger, joiner

**Formulation**:
One way of building a combiner's plan. Two formulations of the same combiner return the same rows
and differ in plan shape, which is what makes them worth having separately: comparing them is the
point of the repo. The reference combiner has a union-of-per-sample-scans formulation; the allele
combiner has one. A CLI subcommand names the combiner and an argument chooses the formulation, so
the two stay separable at the surface as well.
_Avoid_: variant, strategy, combiner (a formulation is not itself a combiner)

**Plan builder**:
The part of a formulation that constructs its DataFrame over a dataset and stops. It does no
runtime setup, object store registration, writing, or execution, which is why plan-shape tests can
assert on it directly. It uses the session it is handed.
_Avoid_: query builder, factory

**Session**:
The DataFusion configuration and state a plan is built against. Every formulation uses the session
it is handed and carries no private session settings.
_Avoid_: context, config (either alone is narrower than what plan shape depends on)

**Plan shape**:
The structure of the physical plan a plan builder produces — which operators appear and how they
nest. Distinct from the plan's results: two plan shapes can be equivalent in output and differ by
an order of magnitude in time. A `SortExec` appearing where a `SortPreservingMergeExec` was
expected is a plan shape regression.
_Avoid_: query plan (ambiguous between logical and physical), execution graph

### Runtimes

**CPU runtime**:
The Tokio runtime a pipeline's plan executes on. Separate from the IO runtime so that plan
execution and object store requests do not compete for the same threads. It has no IO driver, so
a task that attempts IO on it fails rather than quietly taking time from the plan. See
[ADR 0006](docs/adr/0006-run-every-plan-through-the-pipeline-runner.md).
_Avoid_: worker pool, executor, thread pool

**IO runtime**:
The Tokio runtime object store requests run on. It is also the runtime the calling thread blocks
on for the duration of a pipeline, so it outlives the plan's execution.
_Avoid_: network runtime, blocking pool (Tokio's own, separate, thing)

**Runtime flavor**:
Tokio's distinction between a current-thread runtime, driven by whichever thread blocks on it, and
a multi-thread runtime with a fixed number of worker threads. Not a setting here — both of a
pipeline's runtimes are always multi-thread. See
[ADR 0001](docs/adr/0001-always-use-multi-thread-tokio-runtimes.md).
_Avoid_: runtime type, threading mode, runtime kind

### Data

**Sample**:
One sequenced individual, identified by a string id such as `HG00308`. Sample data lives under a
directory named `s=<id>`. The dataset reader attaches that id as a scalar field to the sample's
sorted table.
_Avoid_: individual, subject

**Sample set**:
Which samples a combiner run covers. It is a nonempty property of the dataset rather than of the
caller or the formulation: the samples present under a path are discovered by listing its `s=`
directories, and a caller may narrow that set but neither remove every sample nor extend it. A
formulation may consume the set by building one scan per sample or by leaving the scan to cover all
of them.
_Avoid_: samples (unqualified), sample list, cohort

**Dataset**:
One stored instance of a dataset layout: its path, the format of each file, and the sample set found
there. It lists each sample's files and builds the sorted table that reads them. A formulation may
narrow the sample set, but it does not list paths or assemble readers.
_Avoid_: input, table (a dataset holds many per-sample tables), corpus

**Dataset layout**:
The representation shared by datasets of one kind: their locus ordering. It describes how rows
are arranged across files, separately from how each file is encoded. A formulation declares the
layout it requires after the dataset detects its locus representation.
_Avoid_: format (the encoding of one file), listing options, storage config

**Synthetic table**:
A table of generated rows held in memory, used to exercise a seam without reading genomics data. It
is not genomics-shaped and is not meant to be.
_Avoid_: fixture (fixtures may contain representative genomics data), mock table

**Sorted table**:
A file-backed table that verifies and orders its files from column statistics, then scans them as one
ordered partition. Its partition count belongs to the table rather than the session. It may attach
one scalar field to every row through partition values, but has no knowledge of samples, contigs, or
genomics.
_Avoid_: listing table, sample table, sorted scan

**Locus**:
A position in the genome: a contig together with a position within it. The unit both combiners
order and group by.
_Avoid_: site, coordinate, variant (a variant is a locus plus alleles)

**Locus representation**:
How one stored row records its locus. `contig-position` uses separate `contig` and `position`
fields. `packed` uses one `Int64` `locus` field. A dataset detects the representation from its
resolved schema; callers and formulations do not select it.
_Avoid_: encoding (ambiguous next to compression and Vortex encoding sets), locus format

**Locus ordering**:
The sort order that makes a formulation's plan mergeable rather than re-sorting: `contig` then
`position`, or packed `locus`, optionally followed by `alleles`. The ordering uses stored fields, so
file statistics prove it without relying on directory names. A dataset layout declares the
ordering on disk, and a formulation declares the prefix it requires; a finer ordering satisfies a
coarser requirement.
_Avoid_: sort key, ordering (unqualified)

**Reference data**:
The per-sample records covering loci where a sample matches the reference genome, stored as
runs of adjacent loci rather than one record each. Input to the reference combiner.
_Avoid_: ref blocks, non-variant data

### Formats

**Format**:
The encoding of one file, together with how to read or write it, its extension, and the compression
modes it accepts. It does not describe how a dataset distributes rows across files; that belongs to
the dataset layout. Input and output formats are separate choices because readable formats need not
be writable.
_Avoid_: dataset layout, codec, encoding

### Benchmark settings

**Compression mode**:
A compression choice is always relative to an output format. It names a codec, including its level
when applicable, for Parquet and an encoding scheme set for Vortex, so values are not shared
between formats when recorded for a benchmark.
_Avoid_: compression codec (too narrow for Vortex), encoding (ambiguous without a format)

**Thread count**:
The number of worker threads given to both of a pipeline's runtimes, defaulting to available
parallelism. Independent of the session's target-partition setting.
_Avoid_: parallelism, cores, degree of parallelism
