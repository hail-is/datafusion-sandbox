# Hold the merge tree with the sink's ordering requirement

> Superseded in part by [issue #151](https://github.com/hail-is/datafusion-sandbox/issues/151). `Formulation::plan` returns an ordered frame rather than a plain frame, and the sink still belongs to the run. The rest of this ADR stands.

The grouped-merge formulation of the reference combiner needs the physical plan
`SortPreservingMergeExec(UnionExec(SortPreservingMergeExec(UnionExec(group 1 scans)), ...))`: one
merge per sample group beneath a final merge of the groups, with no sort anywhere. Its plan builder
therefore ends in the union of groups with no sort above it, and every action runs the frame it is
given into a `DataSinkExec` whose required input ordering is the formulation's ordering. The
optimizer builds the merge tree beneath that requirement on its own. This is the decision from
[issue #147](https://github.com/hail-is/datafusion-sandbox/issues/147), implemented in
[issue #139](https://github.com/hail-is/datafusion-sandbox/issues/139).

## Why a final sort cannot hold the group merges

The natural logical plan, `sort(union(sort(union(a, b)), sort(union(c, d))))`, does not become the
nested merge under the DataFusion 55.0.0 `EnsureRequirements` pass this repo builds with. Traced
against `datafusion-physical-optimizer-55.0.0/src/`:

1. Distribution enforcement puts a `CoalescePartitionsExec`, not a merge, beneath every `SortExec`,
   because a sort does not maintain its input order and preserving it is judged pointless
   (`enforce_distribution.rs:1428-1480`).
2. Sorting enforcement removes every inner `SortExec` as neutralized by the outer one
   (`enforce_sorting/mod.rs:741-782`).
3. `parallelize_sorts` recovers only the outermost sort into a merge (`enforce_sorting/mod.rs:319`).

The result is one flat merge over every sample, or under a filter a full sort over a coalesce. A
`SortPreservingMergeExec` placed directly beneath a `UnionExec` by hand is stripped as well, because
`remove_dist_changing_operators` removes every merge and re-adds one only where a parent requires a
single partition (`enforce_distribution.rs:845-871`). The full diagnosis with intermediate plans is
on issue #139.

With no `SortExec` above the group sorts, nothing neutralizes them. The sink's requirement is a
`SinglePartition` distribution plus the ordering; `parallelize_sorts` turns each group sort into a
merge over its union, and the requirement above the union of ordered groups becomes the final
merge. Verified on the fixtures in both formats, with and without a row limit, which becomes a
fetch on every merge.

## Decision

- **Every action runs through a sink.** A write uses the format's file sink, the same
  `DataSinkExec` that `COPY TO` builds. Collect uses a collecting sink that keeps the batches it
  receives, so rows come back in locus order rather than group by group. An analyzed explain
  without a write uses a draining sink that counts rows and drops them. A plain explain renders the
  plan of the sink the action would run: the write's, or the draining sink's without a write. Every
  sink is reached the same way: a `TableProvider` whose `insert_into` builds the sink's execution
  plan with the ordering requirement, behind `LogicalPlanBuilder::insert_into` and a
  `DefaultTableSource`; nothing is registered on the session. Writes do not go through `COPY TO`
  because it derives the requirement from the input's declared ordering, so a frame that lost its
  declaration would be written unordered with no sort and no error.
- **The run supplies every sink's ordering** from the formulation's required layout and the
  dataset's query ordering. `Formulation::plan` keeps returning a plain frame; the sink is the run's
  business, not the plan builder's. Fixtures and tests that write unordered tables place no
  requirement.
- **`--explain` and `--explain-analyze` may combine with `--write`.** Combined, they render or
  analyze the file-sink plan, and analyze performs the write. Validation stays in the CLI per
  [ADR 0004](0004-keep-terminal-defaults-and-validation-in-the-cli.md).
- **The union and allele formulations keep their final sorts.** Their plans are proven, and a sink
  requirement above a sort that already satisfies it changes nothing.

## Considered options

- **An extension logical node planned to a custom execution plan** wrapping a
  `SortPreservingMergeExec`, which the enforcement passes would not recognize as a sort or a
  distribution-changing operator. It works by inspection of the passes and needs a `QueryPlanner`
  with an extension planner registered on every session that plans a grouped read, tests included.
  Deferred, not rejected: it is the candidate if the sink rule fails.
- **A custom physical optimizer rule** rebuilding the nested merges after the built-in passes. It
  needs a marker node to find the group boundaries, so it is the extension node plus a rule.
- **A fetch on the inner sorts**, so that sort removal skips them. Removal skips only `SortExec`s
  with a fetch, and by then the inner sorts are merges, which are removed regardless. Dead end.
- **Changing the DataFusion fork** so that order-preserving variant replacement keeps a merge whose
  child ordering satisfies the parent sort. Reaches outside this repo.
- **Returning a hand-built physical plan from the formulation.** Everything downstream of
  `Formulation::plan` consumes a `DataFrame`: the row limit, the writer, collect, explain, and the
  filtered-plan tests.
- **An opaque leaf execution plan** holding the whole grouped read. Explain and analyze would see
  one node unless it rendered its inner plan and metrics itself, and the plan-shape tests would
  have to look inside it.
- **A post-optimizer session rule** restoring the merges. It is a planner mechanism on every
  session, which is what the sink rule sets out to defer.

## Consequences

Plan-shape tests observe plans through a sink, because a bare frame's physical plan is not the plan
a run executes; the shared loops plan through the draining sink and the file sink both. Every test
that collects grouped-merge rows does so through the collecting sink.
`EXPLAIN` output ends in a `DataSinkExec` naming the sink, and an analyzed explain reports metrics
for the inner merges, which the measurement of parallelism depends on.

The rule rests on two things. First, on how the 55.0.0 enforcement passes treat a sort requirement
above an ordered union, traced on issue #139: an upstream change to sort or distribution enforcement
can break it, and the plan-shape tests are what would notice. Second, on every action having a
sink. A consumer with no sink, such as a caller that executes a formulation's frame directly, gets
its rows group by group. The extension node above is the candidate for either case. No threshold is
set for when grouped-merge earns a mechanism independent of the sink; the point of this rule is to
gather data on DataFusion's runtime behavior first.
