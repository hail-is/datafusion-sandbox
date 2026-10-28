# DRAFT upstream report, to be filed on apache/datafusion by hand

Title: `pushdown_sorts` still pushes a sort through a node that requires a different order when the orders share a key

### Describe the bug

#11678 (for #11675) added `pushdown_would_violate_requirements` so that `pushdown_sorts` won't push a
`SortExec` beneath a node whose own `required_input_ordering` the pushed sort would break. The
guard treats the push as a violation only if **every** zipped key pair is incompatible:

```rust
child_required
    .into_single()
    .iter()
    .zip(parent_required.first().iter())
    .all(|(c, p)| !c.compatible(p))
```

So when the parent's ordering and the node's required ordering agree on any one position, the sort
is pushed through, and the node's requirement no longer holds. `SanityCheckPlan` then rejects the
plan. Checked on 55.1.0. `main` at 56eca4a7c5 has the same code
(`datafusion/physical-optimizer/src/ensure_requirements/enforce_sorting/sort_pushdown.rs:774-792`).

### To Reproduce

A custom node `NeedsOrder` that requires an input order and keeps it (`maintains_input_order =
[true]`), under `SortExec[a, b, c]`, over a table sorted by `(a, b, c)`. The full program
(DataFusion 55.1, one file) is in `upstream-repro/src/main.rs`.

- `NeedsOrder` requires `[b, a]`: every pair conflicts, the guard blocks the push, and the plan is
  valid.
- `NeedsOrder` requires `[b, a, c]`: the third pair (`c`, `c`) is compatible, so the guard lets the
  outer sort through:

```
NeedsOrder: [b@1 ASC, a@0 ASC, c@2 ASC]
  SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]
    SortExec: expr=[b@1 ASC, a@0 ASC, c@2 ASC], preserve_partitioning=[false]
      DataSourceExec: partitions=1, partition_sizes=[1]

ERROR in SanityCheckPlan: Error during planning: Plan: [...] does not satisfy order requirements:
[b@1 ASC, a@0 ASC, c@2 ASC]. Child-0 order: [[a@0 ASC, b@1 ASC, c@2 ASC]]
```

### Expected behavior

The sort stays above `NeedsOrder`, as it does for `[b, a]`. Pushing a requirement through a node
that keeps its input order is safe only if the node's own requirement still holds afterwards. That
holds when one ordering is a prefix of the other, which the guard could check with
`requirements_compatible` (as the `SortExec` branch next to it does), rather than pairwise.

### Additional context

We hit it with two stacked order-asserting nodes. Related: #26697 (custom `ExecutionPlan` nodes).
