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

## Consequences

Changing `target_partitions` may change repartition operators, so plan-shape tests do not require
identical plan text. They require both plans to remain free of `SortExec`. Formulations neither clone
nor modify their caller's session.
