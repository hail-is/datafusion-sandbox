# Preserve declared ordering with a data source wrapper

A sorted table must retain its recovered ordering through physical planning and optimization.
[ADR 0011](0011-recover-file-order-instead-of-proving-it.md) accepts cuts inside a locus under the
assumption that the files hold one sorted table, but declaring that ordering on `FileScanConfig`
alone does not make it survive. We use a delegating data source wrapper to state the ordering while
leaving physical plan creation and execution with the format. This is the ordering decision from
[issue #116](https://github.com/hail-is/datafusion-sandbox/issues/116), implemented in
[issue #117](https://github.com/hail-is/datafusion-sandbox/issues/117).

## Why the config's declaration is not enough

DataFusion re-checks declared orderings against per-file statistics every time `FileScanConfig`
computes its equivalence properties. It uses the composed-row proof that ADR 0011 rejected. Loose
bounds on later columns can make the composed maximum of one file exceed the next file's composed
minimum even when their rows are sorted. A boundary between the alleles of one locus can therefore
pass file order recovery and still lose the scan's ordering. The combiner then sorts instead of
merging.

The check also drops an ordering when file statistics are missing. Single-file groups are exempt,
and touching composed bounds pass. The problem is strict composed overlap, not every shared
boundary. Two files sharing a locus under locus-only ordering remain valid under ADR 0011's
assumptions.

## Decision

The provider still builds a `FileScanConfig` and asks the format to create the physical plan. It
extracts the config from the format's returned `DataSourceExec`, wraps that config in a sorted-table
data source, and rebuilds the `DataSourceExec` around the wrapper. It defines no custom execution
plan. Wrapping the returned config preserves the format's changes during plan creation rather than
replacing them with the provider's original config.

The wrapper delegates behavior to the config except where DataFusion would lose the ordering:

- Equivalence properties declare the recovered ordering without repeating the composed-row proof.
  They retain the config's constraints. The scan retains `UnknownPartitioning(1)`.
- Sort pushdown reports exact when the declared ordering satisfies the requested order, including a
  prefix. Other requests defer to the wrapped config.
- Fetch, projection swap, physical filter pushdown, and repartition re-wrap any rebuilt source.
  Projection must report the ordering in the projected schema, not stale column indices or columns
  no longer present. The declared partitioning keeps repartition attempts from splitting the single
  ordered file group, even under a hostile session.

A format that returns a plan other than a compatible `DataSourceExec` containing a `FileScanConfig`
causes an actionable planning error naming the incompatible plan or source and the required
contract. A rewrite that cannot preserve that contract must also fail rather than silently return
an unwrapped source and allow a sort to appear.

## Consequences and removal

This narrows [ADR 0007](0007-use-a-sorted-table-for-ordered-file-scans.md)'s delegation decision.
There is still no custom execution plan. The data source wrapper exists only to state the recovered
ordering and preserve it through optimizer rewrites. File order recovery, the writer assumptions,
exactness requirements, and string-statistics truncation settings in ADR 0011 do not change.

The wrapper can be deleted when upstream `FileScanConfig` can trust a declared ordering, or uses a
per-column check like our file order recovery that accepts these layouts under the same assumptions.
That capability must preserve the ordering through properties computation, satisfied sort pushdown,
and optimizer rewrites. Filing an upstream issue or maintaining a DataFusion fork is outside this
slice.
