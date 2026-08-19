# datafusion-sandbox

Prototypes of Hail-style genomics pipelines built on DataFusion, and the benchmarks that time them.
The point of the repo is comparing how different formulations of the same query plan perform, so the
shape of a plan is itself a first-class subject here, not just its results.

## Language

### Pipelines

**Pipeline**:
A named end-to-end query over genomics data, from input tables through to its consumed result. A
pipeline is the closure the runner executes: it takes a session, runs to completion on the
pipeline's runtimes, and returns what it produced to the calling thread. CLI combiners return an
Outcome containing written row counts, collected batches, or a plain or analyzed plan; fixture
writers and tests also construct pipelines with other result types. A pipeline result is not a
DataFrame tied to work that has yet to execute.
_Avoid_: job, query (too narrow — a pipeline includes its execution setup), driver

**Outcome**:
What a CLI pipeline hands back after it runs: rows written, collected batches, or a plan rendered
as text. Both plain explain and explain-analyze produce the rendered-plan case.
_Avoid_: result (too broad), output (ambiguous with a written artifact)

**Combiner**:
A pipeline that merges per-sample tables into a single locus-ordered table. The reference combiner
and the allele combiner are the two we have.
_Avoid_: merger, joiner

**Plan builder**:
The async function each combiner exposes to construct its DataFrame. The caller chooses the input
file format; the combiner owns the locus ordering because its plan shape depends on that ordering.
It constructs the plan and nothing else — no runtime setup, object store registration, writing, or
execution. Plan-shape tests assert on this part because it deliberately stops before execution.
Keeping writing out is what distinguishes a plan builder from a pipeline; this is a convention, not
a shared interface.
_Avoid_: query builder, factory

**Plan shape**:
The structure of the physical plan a plan builder produces — which operators appear and how they
nest. Distinct from the plan's results: two plan shapes can be equivalent in output and differ by
an order of magnitude in time. A `SortExec` appearing where a `SortPreservingMergeExec` was
expected is a plan shape regression.
_Avoid_: query plan (ambiguous between logical and physical), execution graph

### Data

**Sample**:
One sequenced individual, identified by a string id such as `HG00308`. Sample data lives under a
directory named `s=<id>`, which DataFusion reads as a partition column.
_Avoid_: individual, subject

**Locus**:
A position in the genome: a contig together with a position within it. The unit both combiners
order and group by.
_Avoid_: site, coordinate, variant (a variant is a locus plus alleles)

**Locus ordering**:
The sort order that makes the combiners' plans mergeable rather than re-sorting: contig, then
position, optionally then alleles. It has to be declared identically on the input files and
requested in the query, or the planner stops believing the inputs are sorted and inserts a sort.
_Avoid_: sort key, ordering (unqualified)

**Reference data**:
The per-sample records covering loci where a sample matches the reference genome, stored as
runs of adjacent loci rather than one record each. Input to the reference combiner.
_Avoid_: ref blocks, non-variant data
