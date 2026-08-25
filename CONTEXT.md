# datafusion-sandbox

Prototypes of Hail-style genomics pipelines built on DataFusion, and the benchmarks that time them.
The point of the repo is comparing how different formulations of the same query plan perform, so the
shape of a plan is itself a first-class subject here, not just its results.

## Language

### Pipelines

**Pipeline**:
A named end-to-end query over genomics data, from input tables through to its consumed result. A
pipeline is the closure the runner executes: it takes a session, runs to completion on the
pipeline's runtimes, and returns what it produced to the calling thread. A combiner run from the
CLI returns an Outcome: written row counts, collected batches, or a plain or analyzed plan; fixture
writers and tests also construct pipelines with other result types. A pipeline result is not a
DataFrame tied to work that has yet to execute.
_Avoid_: job, query (too narrow — a pipeline includes its execution setup), driver

**Outcome**:
What a CLI pipeline hands back after it runs: rows written, collected batches, or a plan rendered
as text. Both plain explain and explain-analyze produce the rendered-plan case.
_Avoid_: result (too broad), output (ambiguous with a written artifact)

**Combiner**:
A merge of per-sample tables into a single locus-ordered table, named by the table it produces.
The reference combiner and the allele combiner are the two we have. A combiner says what is
produced, not how; each is realized by one or more formulations.
_Avoid_: merger, joiner

**Formulation**:
One way of building a combiner's plan. Two formulations of the same combiner return the same rows
and differ in plan shape, which is what makes them worth having separately: comparing them is the
point of the repo. The reference combiner has a union-of-per-sample-scans formulation and a
one-shared-scan formulation; the allele combiner has one. A CLI subcommand names the combiner and
an argument chooses the formulation, so the two stay separable at the surface as well.
_Avoid_: variant, strategy, combiner (a formulation is not itself a combiner)

**Plan builder**:
The part of a formulation that constructs its DataFrame over a dataset and stops. It does no
runtime setup, object store registration, writing, or execution — keeping those out is what
distinguishes a plan builder from a pipeline, and it is why plan-shape tests can assert on a plan
builder directly. A formulation's locus ordering and the session it derives are part of its plan
builder rather than inputs to it, because the plan shape depends on all three together.
_Avoid_: query builder, factory

**Session**:
The DataFusion configuration and state a plan is built against. Part of what decides plan shape,
not merely a performance knob: target partitions and whether file partitions are preserved change
which operators appear. A formulation does not build under whatever session it is handed — it
derives its own, overriding the settings its plan shape depends on and inheriting the rest, so no
caller can change a plan's shape by changing a session. The settings a formulation overrides are
the list of dependencies it has yet to shed.
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
a task that attempts IO on it fails rather than quietly taking time from the plan.
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
directory named `s=<id>`, which DataFusion reads as a partition column.
_Avoid_: individual, subject

**Sample set**:
Which samples a combiner run covers. It is a property of the dataset rather than of the caller or
the formulation: the samples present under a path are discovered by listing its `s=` directories,
and a caller may narrow that set but not extend it. A formulation may consume the set by building
one scan per sample or by leaving the scan to cover all of them.
_Avoid_: samples (unqualified), sample list, cohort

**Dataset**:
What a combiner run reads: a path, the format of the files under it, and the sample set it covers.
One value describing one directory, so which path, which format, and which samples cannot disagree
with each other. A formulation reads a dataset; it does not get to decide what one is.
_Avoid_: input, table (a dataset holds many per-sample tables), corpus

**Locus**:
A position in the genome: a contig together with a position within it. The unit both combiners
order and group by.
_Avoid_: site, coordinate, variant (a variant is a locus plus alleles)

**Locus ordering**:
The sort order that makes a formulation's plan mergeable rather than re-sorting: contig, then
position, optionally then alleles. It has to be declared identically on the input files and
requested in the query, or the planner stops believing the inputs are sorted and inserts a sort.
_Avoid_: sort key, ordering (unqualified)

**Reference data**:
The per-sample records covering loci where a sample matches the reference genome, stored as
runs of adjacent loci rather than one record each. Input to the reference combiner.
_Avoid_: ref blocks, non-variant data

### Formats

**Format**:
The encoding of a table on disk, together with how to read and write it, its file extension, and
the compression modes it accepts. Every format can be read but only some can be written, so the
code represents input and output format choices as separate types.
_Avoid_: file format, codec, encoding

### Benchmark settings

**Compression mode**:
A compression choice is always relative to an output format. It names a codec, including its level
when applicable, for Parquet and an encoding scheme set for Vortex, so values are not shared
between formats when recorded for a benchmark.
_Avoid_: compression codec (too narrow for Vortex), encoding (ambiguous without a format)

**Thread count**:
The number of worker threads given to both of a pipeline's runtimes, defaulting to available
parallelism. Independent of how many partitions a plan is built with, which each formulation
settles for itself, so plan shape and thread count are separate axes.
_Avoid_: parallelism, cores, degree of parallelism
