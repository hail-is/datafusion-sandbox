# Formulations use shared session defaults

Every formulation builds against the session it is handed. The pipeline enables
`optimizer.prefer_existing_sort` on its shared session because the allele formulation feeds sorted
input through `distinct()` and `rank()`. With several target partitions, DataFusion parallelizes
those operators; preserving their input ordering avoids the `SortExec`s that would otherwise make
the plan much slower. The formulation requests its locus ordering again after `rank()`, which merges
the sorted parallel partitions back into global locus order without re-sorting them.

The earlier design derived a session inside each formulation and pinned `target_partitions` to one.
That prevented the sorts, but it also serialized operators that DataFusion can run in parallel and
made session policy part of the plan builder. Sorted tables now pin scan partitioning themselves,
and the shared ordering preference handles the remaining parallel operators, so `derived_session`
and the caller-isolation test are gone.

## Parquet decode-time filtering

The shared session also enables `execution.parquet.pushdown_filters`, and leaves
`execution.parquet.reorder_filters` at its default. A caller restricts a combiner run to a contig
or a locus interval with an ordinary filter. The sorted table prunes files by that filter at plan
time and keeps a residual for the format. Parquet reports the residual exact only when it filters
at decode time. With the setting off, Parquet's scan is followed by a filter operator, the
optimizer places a round-robin repartition beneath the filter to parallelize it, and the merge
widens from one input per sample to one per target partition. Vortex applies the residual inside
its scan by default, so its scan is the only place its filter appears. The setting makes a
filtered Parquet plan match a filtered Vortex plan, so a comparison across formats under a filter
measures the format and not the optimizer's reaction to it. It changes no unfiltered plan.

A filtered plan matches the unfiltered baseline below the union. The reference combiner's filtered
plan is its unfiltered plan. The allele combiner's parallel operators repartition above the union
with or without a filter, and under a filter the optimizer adds one more order-preserving
round-robin repartition there because a filtered scan's row count is inexact. It does so in both
formats alike, so the plans stay comparable across formats.

Filter reordering was left at its default because it changes how Parquet evaluates a filter, not
where the plan places it, and nothing here has measured it.

## Consequences

Changing `target_partitions` may change repartition operators, so plan-shape tests do not require
identical plan text. They require both plans to remain free of `SortExec`. Formulations neither clone
nor modify their caller's session.

A filtered plan keeps one ordered partition per sample into its union and no filter operator, in
both formats. Tests that inspect a plan under a filter build their session from the shared
configuration rather than restating the Parquet setting, so there is one statement of the policy.
