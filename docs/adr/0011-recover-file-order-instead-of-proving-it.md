# Recover file order from statistics instead of proving it

A sorted table orders its files from their ordering statistics, each file's per-column minimum and
maximum. [ADR
0007](0007-use-a-sorted-table-for-ordered-file-scans.md) had it call DataFusion's statistics
grouping, which sorts files by a row composed of each column's minimum and accepts the result only
when each composed maximum is below the next composed minimum. That is a proof, and it is sound. It
is also too weak for compound orderings. Per-column bounds describe a file's rows on the second
ordering column across every value of the first, so when a file boundary falls inside a value of
the leading column the composed bounds are loose and the proof fails on layouts that are in fact
sorted. Hail partitions a table on intervals over its key and may cut anywhere in it, including
between the alleles of one locus. We want to read those layouts.

We give up the proof. A sorted table now assumes its files hold one sorted table and recovers the
only order compatible with that assumption, rejecting the files when their statistics refute it.
Per column, two files compare as intervals: a file is at or before another when its maximum is at
most the other's minimum; two files are equivalent when that holds both ways, so both are constant
at the same value; otherwise they are incomparable. The file order is the lexicographic order over
the ordering columns built from these comparisons, and an incomparable pair at any column is a
refutation. The implementation sorts by the interleaved minimum and maximum of each column, reading
a column's bounds only for files constant on the columns before it, then checks each adjacent pair
with the three-way comparison. Transitivity of the interval relation makes a consistent chain of
adjacent pairs a consistent whole. The sort uses a proxy key rather than the interval comparison
because that comparison is a total order only when a sorted order exists, and the standard sort may
panic on a comparator that is not one.

The recovered order trusts the writer about one thing it cannot see. In a run of files sharing a
leading value, at most one file may extend past that value, and its statistics on later columns say
nothing about its rows at the shared value. The proof refused this trust. The proof is one change
away in the comparison: require the maximum to be strictly below the next minimum, so a constant
file next to a non-constant one at the same value becomes incomparable. The code marks the spot.

Because the recovered order trusts bounds where the proof did not, bounds must be exact where they
are compared. A constant file whose bounds are inexact looks non-constant, would be placed last in
its run, and could land after files whose rows follow it, with nothing at plan time to notice. A
file's bounds on a column are compared only when the file is constant on every earlier column, so
exactness is required only there and is checked at the comparison. Null counts do not matter.

## Consequences

Sorted tables accept any layout that is sorted, including cuts inside a locus, and reject only
statistics that contradict every sorted order. Refutations name both files. Missing or inexact
bounds name the file and the column.

Both formats we write truncate string statistics at 64 bytes and mark them inexact. A file constant
on the full locus with an allele longer than that is rejected under either format. We leave the
limit in place so the formats agree. If it bites, the Parquet fix is to set the session's parquet
statistics truncation length to none, which the string-keyed format options cannot express; the
Vortex fix is a setter for the private cap in our fork, routed through its DataFusion sink.

Files with an exact row count of zero are dropped before ordering. Files with null values in an
ordering column have no bound for those rows and are unsupported. Descending ordering columns swap
the roles of minimum and maximum.

ADR 0007's paragraph on how files are ordered and checked is superseded by this one. The rest of
0007 stands: one ordered partition, `UnknownPartitioning(1)`, plan-time errors that name a file, no
custom execution plan.
